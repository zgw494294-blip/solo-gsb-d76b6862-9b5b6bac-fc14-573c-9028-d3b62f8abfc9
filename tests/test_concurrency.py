"""Concurrency tests: genuinely parallel requests via OS threads + event loops."""

import threading
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from app.main import app

from .conftest import run


async def _register_and_open(base_url):
    async with httpx.AsyncClient(base_url=base_url) as c:
        r = await c.put(
            "/groups/g1",
            json={"devices": {f"dev-{i}": "1.0.0" for i in range(4)}},
        )
        assert r.status_code == 200, r.text
        r = await c.post(
            "/groups/g1/batches",
            json={"targets": {f"dev-{i}": "2.0.0" for i in range(4)}},
        )
        assert r.status_code == 201, r.text
        return r.json()["batch_id"]


def _put_receipt(base_url, batch_id, device, result):
    async def go():
        async with httpx.AsyncClient(base_url=base_url) as c:
            return await c.put(
                f"/batches/{batch_id}/devices/{device}/receipt",
                json={"result": result},
            )

    return run(go())


@pytest.mark.asyncio
async def test_all_ready_concurrently_equivalent_to_serial(app_db, unused_tcp_port_factory):
    # Drive the real uvicorn server over TCP so requests run on real threads.
    import uvicorn

    port = unused_tcp_port_factory()
    base_url = f"http://127.0.0.1:{port}"
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)

    ready = threading.Event()
    original_startup = server.startup

    async def startup_with_signal(*args, **kwargs):
        await original_startup(*args, **kwargs)
        ready.set()

    server.startup = startup_with_signal
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    assert ready.wait(timeout=10)

    try:
        batch_id = await _register_and_open(base_url)

        with ThreadPoolExecutor(max_workers=4) as pool:
            responses = list(
                pool.map(
                    lambda d: _put_receipt(base_url, batch_id, d, "ready"),
                    [f"dev-{i}" for i in range(4)],
                )
            )

        statuses = sorted(r.status_code for r in responses)
        assert statuses == [200, 200, 200, 200]
        final = [r.json()["status"] for r in responses]
        assert final.count("succeeded") == 1
        assert final.count("pending") == 3

        async with httpx.AsyncClient(base_url=base_url) as c:
            group = (await c.get("/groups/g1")).json()
            assert {d["current_version"] for d in group["devices"]} == {"2.0.0"}
            assert group["open_batch_id"] is None
            batch = (await c.get(f"/batches/{batch_id}")).json()
            assert batch["status"] == "succeeded"
            assert len(batch["receipts"]) == 4
    finally:
        server.should_exit = True
        thread.join(timeout=5)


@pytest.mark.asyncio
async def test_mixed_ready_failed_concurrent(app_db, unused_tcp_port_factory):
    import uvicorn

    port = unused_tcp_port_factory()
    base_url = f"http://127.0.0.1:{port}"
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    )
    ready = threading.Event()
    original_startup = server.startup

    async def startup_with_signal(*args, **kwargs):
        await original_startup(*args, **kwargs)
        ready.set()

    server.startup = startup_with_signal
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    assert ready.wait(timeout=10)

    try:
        batch_id = await _register_and_open(base_url)

        # Two ready and one failed flying at the same time; 4th device never reports.
        jobs = {
            "dev-0": "ready",
            "dev-1": "ready",
            "dev-2": "failed",
        }
        with ThreadPoolExecutor(max_workers=3) as pool:
            responses = list(
                pool.map(lambda kv: _put_receipt(base_url, batch_id, *kv), jobs.items())
            )
        by_device = {
            d: r for d, r in zip(jobs, responses)
        }
        # The failure is always accepted and ends the batch.
        assert by_device["dev-2"].status_code == 200
        # A ready that serializes after the failure is a late *new* receipt on an
        # ended batch -> 409; one that serialized before it -> 200. Both outcomes
        # are consistent with some serial ordering.
        for d in ("dev-0", "dev-1"):
            assert by_device[d].status_code in (200, 409), by_device[d].text

        async with httpx.AsyncClient(base_url=base_url) as c:
            batch = (await c.get(f"/batches/{batch_id}")).json()
            assert batch["status"] == "failed"
            group = (await c.get("/groups/g1")).json()
            # failed batch: no version changes at all
            assert {d["current_version"] for d in group["devices"]} == {"1.0.0"}
            assert group["open_batch_id"] is None

            # new batch can open immediately after the failed one
            r = await c.post(
                "/groups/g1/batches",
                json={"targets": {f"dev-{i}": "3.0.0" for i in range(4)}},
            )
            assert r.status_code == 201
    finally:
        server.should_exit = True
        thread.join(timeout=5)


@pytest.mark.asyncio
async def test_same_device_same_result_replay_race(app_db, unused_tcp_port_factory):
    import uvicorn

    port = unused_tcp_port_factory()
    base_url = f"http://127.0.0.1:{port}"
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    )
    ready = threading.Event()
    original_startup = server.startup

    async def startup_with_signal(*args, **kwargs):
        await original_startup(*args, **kwargs)
        ready.set()

    server.startup = startup_with_signal
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    assert ready.wait(timeout=10)

    try:
        batch_id = await _register_and_open(base_url)

        with ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(
                pool.map(
                    lambda _: _put_receipt(base_url, batch_id, "dev-0", "ready"),
                    range(2),
                )
            )
        codes = sorted(r.status_code for r in responses)
        assert codes == [200, 200]
        replays = [r.json()["replayed"] for r in responses]
        assert sorted(replays) == [False, True]

        # divergent results racing: exactly one wins, other gets 409
        with ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(
                pool.map(
                    lambda res: _put_receipt(base_url, batch_id, "dev-1", res),
                    ["ready", "failed"],
                )
            )
        codes = sorted(r.status_code for r in responses)
        assert codes == [200, 409]

        async with httpx.AsyncClient(base_url=base_url) as c:
            batch = (await c.get(f"/batches/{batch_id}")).json()
            # A failed receipt may or may not be the winner on dev-1,
            # but the stored state must be internally consistent either way.
            assert batch["status"] in {"pending", "failed"}
            if batch["status"] == "failed":
                group = (await c.get("/groups/g1")).json()
                assert {d["current_version"] for d in group["devices"]} == {"1.0.0"}
    finally:
        server.should_exit = True
        thread.join(timeout=5)


@pytest.mark.asyncio
async def test_concurrent_batch_creation_single_winner(app_db, unused_tcp_port_factory):
    import uvicorn

    port = unused_tcp_port_factory()
    base_url = f"http://127.0.0.1:{port}"
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    )
    ready = threading.Event()
    original_startup = server.startup

    async def startup_with_signal(*args, **kwargs):
        await original_startup(*args, **kwargs)
        ready.set()

    server.startup = startup_with_signal
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    assert ready.wait(timeout=10)

    async def register():
        async with httpx.AsyncClient(base_url=base_url) as c:
            r = await c.put(
                "/groups/g1",
                json={"devices": {f"dev-{i}": "1.0.0" for i in range(2)}},
            )
            assert r.status_code == 200

    try:
        await register()

        def create(_):
            async def go():
                async with httpx.AsyncClient(base_url=base_url) as c:
                    return await c.post(
                        "/groups/g1/batches",
                        json={"targets": {f"dev-{i}": "2.0.0" for i in range(2)}},
                    )

            return run(go())

        with ThreadPoolExecutor(max_workers=3) as pool:
            responses = list(pool.map(create, range(3)))

        codes = sorted(r.status_code for r in responses)
        assert codes == [201, 409, 409]
    finally:
        server.should_exit = True
        thread.join(timeout=5)
