"""持久化聚合对账 GET /devices/{device_id}/integrity 测试。

覆盖：正常聚合对账无差异、缺少窗口行、无事件的窗口行、count/sum/min/max/
late_event_count 各项不符（含浮点容差）、迟到事件计入重算、正常空窗不报差异、
范围半开边界、对齐/倒置/超 1000 窗口 422、设备不存在 404、纯只读（水位线、
窗口、修正、组汇总均不变）。
"""

import sqlite3


def make_device(client, device_id="dev-1", window_size_seconds=60, allowed_lateness_seconds=30):
    r = client.put(
        f"/devices/{device_id}/config",
        json={
            "window_size_seconds": window_size_seconds,
            "allowed_lateness_seconds": allowed_lateness_seconds,
        },
    )
    assert r.status_code == 200, r.text


def ingest(client, device_id, events):
    r = client.post("/ingest", json={"device_id": device_id, "events": events})
    assert r.status_code == 200, r.text
    return r


def integrity(client, device_id, start, end):
    return client.get(
        f"/devices/{device_id}/integrity",
        params={"start": start, "end": end},
    )


def db_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ---------- 无差异 ----------

def test_clean_aggregations_have_no_discrepancies(client, tmp_path, monkeypatch):
    make_device(client)
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:00:40Z", "value": 3.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:01:05Z", "value": 7.0},
        # 抬高水位线封存前面窗口
        {"event_id": "e4", "event_time": "2026-09-22T00:03:00Z", "value": 1.0},
    ])
    r = integrity(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["checked_windows"] == 10
    assert body["discrepancy_count"] == 0
    assert body["discrepancies"] == []
    assert body["window_size_seconds"] == 60


def test_late_events_are_recomputed_into_expected(client):
    make_device(client)
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:03:00Z", "value": 2.0},
    ])
    # 迟到事件落入已封存的 00:00 窗口
    r = ingest(client, "dev-1", [
        {"event_id": "e3", "event_time": "2026-09-22T00:00:20Z", "value": 9.0}
    ])
    assert r.json()["results"][0]["status"] == "late"

    body = integrity(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:01:00Z").json()
    assert body["checked_windows"] == 1
    assert body["discrepancy_count"] == 0


def test_empty_positions_without_event_or_window_row_are_not_discrepancies(client):
    make_device(client)
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:03:00Z", "value": 1.0},
    ])
    # 00:01、00:02 既无事件也无窗口行——正常空缺，不列为差异
    body = integrity(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:05:00Z").json()
    assert body["checked_windows"] == 5
    assert body["discrepancies"] == []


# ---------- 结构差异：缺少窗口行 ----------

def test_missing_window_row_is_reported(client):
    from datetime import datetime, timezone
    make_device(client)
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:00:50Z", "value": 3.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:03:00Z", "value": 1.0},
    ])

    ws = datetime(2026, 9, 22, 0, 0, tzinfo=timezone.utc).timestamp()
    with db_conn(str(tmp_db_path(client))) as c:
        c.execute("DELETE FROM windows WHERE device_id=? AND window_start=?", ("dev-1", ws))

    body = integrity(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:05:00Z").json()
    assert body["discrepancy_count"] == 1
    item = body["discrepancies"][0]
    assert item["kind"] == "missing_window_row"
    assert item["window_start"] == "2026-09-22T00:00:00Z"
    assert item["window_end"] == "2026-09-22T00:01:00Z"
    assert item["actual"] is None
    assert item["expected"] == {
        "count": 2, "sum": 4.0, "min": 1.0, "max": 3.0, "late_event_count": 0
    }
    assert item["fields"] == []


def tmp_db_path(client):
    from app import db as db_mod
    return db_mod.db_path()


# ---------- 结构差异：无事件的窗口行 ----------

def test_empty_window_row_is_reported(client):
    from datetime import datetime, timezone
    make_device(client)
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:03:00Z", "value": 1.0},
    ])
    ws = datetime(2026, 9, 22, 0, 2, tzinfo=timezone.utc).timestamp()
    with db_conn(str(tmp_db_path(client))) as c:
        c.execute(
            "INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,"
            " status, correction_count, late_event_count) VALUES (?, ?, ?, 0, 0, NULL, NULL,"
            " 'sealed', 0, 0)",
            ("dev-1", ws, ws + 60),
        )

    body = integrity(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:05:00Z").json()
    assert body["discrepancy_count"] == 1
    item = body["discrepancies"][0]
    assert item["kind"] == "empty_window_row"
    assert item["window_start"] == "2026-09-22T00:02:00Z"
    assert item["expected"] == {
        "count": 0, "sum": 0.0, "min": None, "max": None, "late_event_count": 0
    }
    assert item["actual"] == {
        "count": 0, "sum": 0.0, "min": None, "max": None, "late_event_count": 0
    }
    assert item["fields"] == []


# ---------- 数值差异 ----------

def _corrupt_window(client, device_id, ws_iso, **cols):
    from datetime import datetime, timezone
    ws = datetime.fromisoformat(ws_iso.replace("Z", "+00:00")).timestamp()
    sets = ", ".join(f"{k} = ?" for k in cols)
    with db_conn(str(tmp_db_path(client))) as c:
        c.execute(
            f"UPDATE windows SET {sets} WHERE device_id=? AND window_start=?",
            (*cols.values(), device_id, ws),
        )


def test_aggregate_mismatch_reports_each_field(client):
    make_device(client)
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:00:40Z", "value": 3.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:03:00Z", "value": 1.0},
    ])
    _corrupt_window(
        client, "dev-1", "2026-09-22T00:00:00Z",
        count=5, sum=99.0, min=0.5, max=42.0, late_event_count=2,
    )

    body = integrity(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:01:00Z").json()
    assert body["checked_windows"] == 1
    item = body["discrepancies"][0]
    assert item["kind"] == "aggregate_mismatch"
    assert item["fields"] == ["count", "sum", "min", "max", "late_event_count"]
    assert item["expected"] == {
        "count": 2, "sum": 4.0, "min": 1.0, "max": 3.0, "late_event_count": 0
    }
    assert item["actual"] == {
        "count": 5, "sum": 99.0, "min": 0.5, "max": 42.0, "late_event_count": 2
    }


def test_late_event_count_mismatch_alone(client):
    make_device(client)
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:03:00Z", "value": 1.0},
    ])
    _corrupt_window(client, "dev-1", "2026-09-22T00:00:00Z", late_event_count=3)
    body = integrity(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:01:00Z").json()
    item = body["discrepancies"][0]
    assert item["kind"] == "aggregate_mismatch"
    assert item["fields"] == ["late_event_count"]
    assert item["expected"]["late_event_count"] == 0
    assert item["actual"]["late_event_count"] == 3


def test_float_tolerance_within_1e_9_is_not_a_discrepancy(client):
    make_device(client)
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:03:00Z", "value": 1.0},
    ])
    # 绝对误差 <= 1e-9：视为一致（count 等整数字段不变）
    _corrupt_window(client, "dev-1", "2026-09-22T00:00:00Z", sum=1.0 + 1e-10, max=1.0 + 5e-10)
    body = integrity(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:01:00Z").json()
    assert body["discrepancies"] == []

    _corrupt_window(client, "dev-1", "2026-09-22T00:00:00Z", sum=1.0 + 2e-9)
    body = integrity(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:01:00Z").json()
    assert body["discrepancies"][0]["fields"] == ["sum"]


def test_discrepancies_sorted_by_window_start(client):
    make_device(client)
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:01:10Z", "value": 1.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:02:10Z", "value": 1.0},
        {"event_id": "e4", "event_time": "2026-09-22T00:04:00Z", "value": 1.0},
    ])
    _corrupt_window(client, "dev-1", "2026-09-22T00:02:00Z", count=99)
    _corrupt_window(client, "dev-1", "2026-09-22T00:00:00Z", count=99)
    _corrupt_window(client, "dev-1", "2026-09-22T00:01:00Z", count=99)

    body = integrity(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:05:00Z").json()
    assert [d["window_start"] for d in body["discrepancies"]] == [
        "2026-09-22T00:00:00Z",
        "2026-09-22T00:01:00Z",
        "2026-09-22T00:02:00Z",
    ]


# ---------- 范围边界 ----------

def test_half_open_range_boundaries(client):
    make_device(client)
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:01:00Z", "value": 5.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:03:00Z", "value": 1.0},
    ])
    # [00:00, 00:01)：只覆盖 00:00 窗口
    body = integrity(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:01:00Z").json()
    assert body["checked_windows"] == 1
    assert [d["window_start"] for d in body["discrepancies"]] == []

    # [00:01, 00:03)：覆盖 00:01、00:02；00:00 窗口行不在查询范围内
    body = integrity(client, "dev-1", "2026-09-22T00:01:00Z", "2026-09-22T00:03:00Z").json()
    assert body["checked_windows"] == 2
    assert body["discrepancies"] == []


def test_custom_window_size_grid(client):
    make_device(client, "dev-big", window_size_seconds=300)
    ingest(client, "dev-big", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 2.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:20:00Z", "value": 1.0},
    ])
    body = integrity(client, "dev-big", "2026-09-22T00:00:00Z", "2026-09-22T00:25:00Z").json()
    assert body["window_size_seconds"] == 300
    assert body["checked_windows"] == 5
    assert body["discrepancies"] == []


# ---------- 参数校验与错误码 ----------

def test_missing_device_returns_404(client):
    r = integrity(client, "ghost", "2026-09-22T00:00:00Z", "2026-09-22T01:00:00Z")
    assert r.status_code == 404


def test_inverted_or_empty_range_is_422(client):
    make_device(client)
    assert integrity(
        client, "dev-1", "2026-09-22T01:00:00Z", "2026-09-22T00:00:00Z"
    ).status_code == 422
    assert integrity(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:00:00Z"
    ).status_code == 422


def test_misaligned_boundaries_are_422(client):
    make_device(client)
    assert integrity(
        client, "dev-1", "2026-09-22T00:00:30Z", "2026-09-22T01:00:00Z"
    ).status_code == 422
    assert integrity(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:59:59Z"
    ).status_code == 422


def test_missing_required_params_are_422(client):
    make_device(client)
    assert client.get("/devices/dev-1/integrity").status_code == 422
    assert client.get(
        "/devices/dev-1/integrity", params={"start": "2026-09-22T00:00:00Z"}
    ).status_code == 422


def test_range_over_1000_windows_is_422(client):
    make_device(client)
    # 恰好 1000 个窗口（1000 分钟 = 16h40m）：允许
    r = integrity(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T16:40:00Z"
    )
    assert r.status_code == 200, r.text
    assert r.json()["checked_windows"] == 1000
    # 1001 个窗口：422
    r = integrity(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T16:41:00Z"
    )
    assert r.status_code == 422
    # 未对齐边界同样 422（1h40m10s 不对齐 60s 网格）
    assert integrity(
        client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T01:40:10Z"
    ).status_code == 422


def test_over_limit_with_custom_window_size(client):
    make_device(client, "dev-big", window_size_seconds=3600)
    # 3600s 网格：上限跨度 1000 小时
    assert integrity(
        client, "dev-big", "2026-09-01T00:00:00Z", "2066-09-01T01:00:00Z"
    ).status_code == 422


# ---------- 只读语义 ----------

def test_integrity_check_is_readonly(client):
    make_device(client)
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:00:40Z", "value": 3.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:03:00Z", "value": 1.0},
    ])
    # 迟到事件制造修正
    ingest(client, "dev-1", [
        {"event_id": "e4", "event_time": "2026-09-22T00:00:20Z", "value": 9.0}
    ])

    before_device = client.get("/devices/dev-1").json()
    before_windows = client.get("/devices/dev-1/windows").json()
    before_corrections = client.get("/devices/dev-1/corrections").json()

    for _ in range(3):
        integrity(client, "dev-1", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z")
    # 错误路径同样不得写库
    integrity(client, "ghost", "2026-09-22T00:00:00Z", "2026-09-22T00:10:00Z")
    integrity(client, "dev-1", "2026-09-22T00:00:30Z", "2026-09-22T00:10:00Z")

    assert client.get("/devices/dev-1").json() == before_device
    assert client.get("/devices/dev-1/windows").json() == before_windows
    assert client.get("/devices/dev-1/corrections").json() == before_corrections
