import pytest


async def register(client, group_id="g1"):
    resp = await client.put(
        f"/groups/{group_id}",
        json={"devices": {"dev-a": "1.0.0", "dev-b": "1.0.0", "dev-c": "1.0.0"}},
    )
    assert resp.status_code == 200


async def open_batch(client, group_id="g1", targets=None):
    targets = targets or {"dev-a": "2.0.0", "dev-b": "2.0.0", "dev-c": "2.0.0"}
    resp = await client.post(f"/groups/{group_id}/batches", json={"targets": targets})
    assert resp.status_code == 201, resp.text
    return resp.json()["batch_id"]


@pytest.mark.asyncio
async def test_create_batch_and_get(client):
    await register(client)
    batch_id = await open_batch(client)

    resp = await client.get(f"/batches/{batch_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "pending"
    assert set(body["targets"]) == {"dev-a", "dev-b", "dev-c"}
    assert body["receipts"] == []

    group = await client.get("/groups/g1")
    assert group.json()["open_batch_id"] == batch_id


@pytest.mark.asyncio
async def test_open_batch_blocked_while_one_pending(client):
    await register(client)
    await open_batch(client)
    resp = await client.post(
        "/groups/g1/batches",
        json={"targets": {"dev-a": "3.0.0", "dev-b": "3.0.0", "dev-c": "3.0.0"}},
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_targets_must_cover_all_devices(client):
    await register(client)
    resp = await client.post(
        "/groups/g1/batches",
        json={"targets": {"dev-a": "2.0.0", "dev-b": "2.0.0"}},
    )
    assert resp.status_code == 422

    resp = await client.post(
        "/groups/g1/batches",
        json={"targets": {"dev-a": "2.0.0", "dev-b": "2.0.0", "dev-c": "2.0.0", "dev-x": "2.0.0"}},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_full_ready_flow_switches_all_versions_atomically(client):
    await register(client)
    batch_id = await open_batch(client)

    for i, dev in enumerate(["dev-a", "dev-b"]):
        resp = await client.put(
            f"/batches/{batch_id}/devices/{dev}/receipt", json={"result": "ready"}
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "pending"
        assert resp.json()["replayed"] is False
        # not switched yet
        group = (await client.get("/groups/g1")).json()
        assert {d["current_version"] for d in group["devices"]} == {"1.0.0"}

    resp = await client.put(
        f"/batches/{batch_id}/devices/dev-c/receipt", json={"result": "ready"}
    )
    body = resp.json()
    assert body["status"] == "succeeded"
    assert body["ended_at"] is not None

    # all versions switched, no partial state observable
    group = (await client.get("/groups/g1")).json()
    assert {d["device_id"]: d["current_version"] for d in group["devices"]} == {
        "dev-a": "2.0.0",
        "dev-b": "2.0.0",
        "dev-c": "2.0.0",
    }
    assert group["open_batch_id"] is None

    # per-device target versions are honored
    b2 = await open_batch(
        client,
        targets={"dev-a": "3.0.0", "dev-b": "2.5.0", "dev-c": "2.0.1"},
    )
    for dev in ["dev-a", "dev-b", "dev-c"]:
        await client.put(f"/batches/{b2}/devices/{dev}/receipt", json={"result": "ready"})
    group = (await client.get("/groups/g1")).json()
    assert {d["device_id"]: d["current_version"] for d in group["devices"]} == {
        "dev-a": "3.0.0",
        "dev-b": "2.5.0",
        "dev-c": "2.0.1",
    }


@pytest.mark.asyncio
async def test_any_failed_fails_batch_and_keeps_versions(client):
    await register(client)
    batch_id = await open_batch(client)

    await client.put(f"/batches/{batch_id}/devices/dev-a/receipt", json={"result": "ready"})
    resp = await client.put(
        f"/batches/{batch_id}/devices/dev-b/receipt", json={"result": "failed"}
    )
    assert resp.json()["status"] == "failed"

    # versions unchanged even after a later ready replay-style arrival
    group = (await client.get("/groups/g1")).json()
    assert {d["current_version"] for d in group["devices"]} == {"1.0.0"}

    batch = (await client.get(f"/batches/{batch_id}")).json()
    assert batch["status"] == "failed"

    # a new batch can now be opened
    resp = await client.post(
        "/groups/g1/batches",
        json={"targets": {"dev-a": "2.0.0", "dev-b": "2.0.0", "dev-c": "2.0.0"}},
    )
    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_replay_same_result_succeeds_and_changing_result_conflicts(client):
    await register(client)
    batch_id = await open_batch(client)

    url = f"/batches/{batch_id}/devices/dev-a/receipt"
    r1 = await client.put(url, json={"result": "ready"})
    assert r1.status_code == 200 and r1.json()["replayed"] is False

    r2 = await client.put(url, json={"result": "ready"})
    assert r2.status_code == 200
    assert r2.json()["replayed"] is True

    r3 = await client.put(url, json={"result": "failed"})
    assert r3.status_code == 409

    # failed -> failed replay ok; ready now conflicts
    url_b = f"/batches/{batch_id}/devices/dev-b/receipt"
    f1 = await client.put(url_b, json={"result": "failed"})
    assert f1.status_code == 200
    f2 = await client.put(url_b, json={"result": "failed"})
    assert f2.status_code == 200 and f2.json()["replayed"] is True
    f3 = await client.put(url_b, json={"result": "ready"})
    assert f3.status_code == 409


@pytest.mark.asyncio
async def test_late_new_receipt_after_end_is_rejected_and_isolated(client):
    await register(client)
    b1 = await open_batch(client)

    # dev-c never reports; dev-b fails, ending the batch
    await client.put(f"/batches/{b1}/devices/dev-a/receipt", json={"result": "ready"})
    await client.put(f"/batches/{b1}/devices/dev-b/receipt", json={"result": "failed"})

    # late *new* receipt from a device that never reported -> rejected
    late = await client.put(
        f"/batches/{b1}/devices/dev-c/receipt", json={"result": "ready"}
    )
    assert late.status_code == 409

    # replay of an existing receipt stays accepted
    replay = await client.put(
        f"/batches/{b1}/devices/dev-b/receipt", json={"result": "failed"}
    )
    assert replay.status_code == 200
    assert replay.json()["replayed"] is True
    assert replay.json()["status"] == "failed"

    # follow-up batch must not be influenced by the rejected late receipt
    b2 = (
        await client.post(
            "/groups/g1/batches",
            json={"targets": {"dev-a": "2.0.0", "dev-b": "2.0.0", "dev-c": "2.0.0"}},
        )
    ).json()["batch_id"]

    # dev-c still has to report in b2 for success
    await client.put(f"/batches/{b2}/devices/dev-a/receipt", json={"result": "ready"})
    await client.put(f"/batches/{b2}/devices/dev-b/receipt", json={"result": "ready"})
    pending = (await client.get(f"/batches/{b2}")).json()
    assert pending["status"] == "pending"
    await client.put(f"/batches/{b2}/devices/dev-c/receipt", json={"result": "ready"})
    assert (await client.get(f"/batches/{b2}")).json()["status"] == "succeeded"


@pytest.mark.asyncio
async def test_batch_listing_and_unknown(client):
    await register(client)
    b1 = await open_batch(client)
    await client.put(f"/batches/{b1}/devices/dev-a/receipt", json={"result": "failed"})
    b2 = await open_batch(client)

    resp = await client.get("/groups/g1/batches")
    assert resp.status_code == 200
    batches = resp.json()["batches"]
    assert [b["batch_id"] for b in batches] == [b1, b2]
    assert batches[0]["status"] == "failed"
    assert batches[1]["status"] == "pending"

    assert (await client.get("/batches/nope")).status_code == 404
    assert (
        await client.post(
            "/groups/nope/batches",
            json={"targets": {"a": "1"}},
        )
    ).status_code == 404


@pytest.mark.asyncio
async def test_receipt_for_unknown_device(client):
    await register(client)
    batch_id = await open_batch(client)
    resp = await client.put(
        f"/batches/{batch_id}/devices/dev-x/receipt", json={"result": "ready"}
    )
    assert resp.status_code == 404
