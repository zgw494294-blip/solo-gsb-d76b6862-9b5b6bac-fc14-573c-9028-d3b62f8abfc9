import pytest


@pytest.mark.asyncio
async def test_register_and_get_group(client):
    resp = await client.put(
        "/groups/g1",
        json={"devices": {"dev-a": "1.0.0", "dev-b": "1.0.1", "dev-c": "1.0.0"}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["group_id"] == "g1"
    assert {d["device_id"]: d["current_version"] for d in body["devices"]} == {
        "dev-a": "1.0.0",
        "dev-b": "1.0.1",
        "dev-c": "1.0.0",
    }

    resp = await client.get("/groups/g1")
    assert resp.status_code == 200
    assert resp.json()["open_batch_id"] is None


@pytest.mark.asyncio
async def test_reregister_same_is_idempotent(client):
    payload = {"devices": {"dev-a": "1.0.0", "dev-b": "1.0.0"}}
    first = await client.put("/groups/g1", json=payload)
    assert first.status_code == 200
    created = first.json()["created_at"]

    second = await client.put("/groups/g1", json=payload)
    assert second.status_code == 200
    assert second.json()["created_at"] == created


@pytest.mark.asyncio
async def test_reregister_with_changed_versions_conflicts(client):
    await client.put("/groups/g1", json={"devices": {"dev-a": "1.0.0", "dev-b": "1.0.0"}})
    resp = await client.put("/groups/g1", json={"devices": {"dev-a": "2.0.0", "dev-b": "1.0.0"}})
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_reregister_with_changed_membership_conflicts(client):
    await client.put("/groups/g1", json={"devices": {"dev-a": "1.0.0", "dev-b": "1.0.0"}})
    resp = await client.put(
        "/groups/g1", json={"devices": {"dev-a": "1.0.0", "dev-c": "1.0.0"}}
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_group_size_limits(client):
    one = {f"dev-{i}": "1.0" for i in range(1)}
    resp = await client.put("/groups/g1", json={"devices": one})
    assert resp.status_code == 422

    nine = {f"dev-{i}": "1.0" for i in range(9)}
    resp = await client.put("/groups/g2", json={"devices": nine})
    assert resp.status_code == 422

    two = {f"dev-{i}": "1.0" for i in range(2)}
    resp = await client.put("/groups/g3", json={"devices": two})
    assert resp.status_code == 200

    eight = {f"dev-{i}": "1.0" for i in range(8)}
    resp = await client.put("/groups/g4", json={"devices": eight})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_get_unknown_group_404(client):
    resp = await client.get("/groups/nope")
    assert resp.status_code == 404
