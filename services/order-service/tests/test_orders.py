def test_create_order(client):
    response = client.post(
        "/orders",
        json={
            "user_id": 1,
            "total_amount": "2500.00",
        },
    )

    assert response.status_code == 201

    data = response.json()

    assert data["user_id"] == 1
    assert data["status"] == "pending"
    assert data["total_amount"] == "2500.00"
    assert "id" in data
    assert "created_at" in data


def test_get_order(client):
    create_response = client.post(
        "/orders",
        json={
            "user_id": 2,
            "total_amount": "1500.00",
        },
    )

    order_id = create_response.json()["id"]

    response = client.get(
        f"/orders/{order_id}"
    )

    assert response.status_code == 200
    assert response.json()["id"] == order_id


def test_get_missing_order_returns_404(client):
    response = client.get("/orders/999999")

    assert response.status_code == 404

    data = response.json()

    assert data["error"] == "order_not_found"
    assert data["order_id"] == 999999


def test_cancel_order(client):
    create_response = client.post(
        "/orders",
        json={
            "user_id": 3,
            "total_amount": "3000.00",
        },
    )

    order_id = create_response.json()["id"]

    cancel_response = client.patch(
        f"/orders/{order_id}/cancel"
    )

    assert cancel_response.status_code == 200

    data = cancel_response.json()

    assert data["id"] == order_id
    assert data["status"] == "cancelled"


def test_cancelled_order_is_not_in_order_list(client):
    create_response = client.post(
        "/orders",
        json={
            "user_id": 4,
            "total_amount": "4500.00",
        },
    )

    order_id = create_response.json()["id"]

    cancel_response = client.patch(
        f"/orders/{order_id}/cancel"
    )

    assert cancel_response.status_code == 200
    assert cancel_response.json()["status"] == "cancelled"

    list_response = client.get("/orders")

    assert list_response.status_code == 200

    data = list_response.json()

    returned_ids = [
        order["id"]
        for order in data["items"]
    ]

    assert order_id not in returned_ids


def test_cancelled_order_cannot_be_cancelled_again(client):
    create_response = client.post(
        "/orders",
        json={
            "user_id": 5,
            "total_amount": "5000.00",
        },
    )

    order_id = create_response.json()["id"]

    first_cancel = client.patch(
        f"/orders/{order_id}/cancel"
    )

    assert first_cancel.status_code == 200

    second_cancel = client.patch(
        f"/orders/{order_id}/cancel"
    )

    assert second_cancel.status_code == 409

    data = second_cancel.json()

    assert data["error"] == "invalid_order_status"
    assert data["order_id"] == order_id
    assert data["current_status"] == "cancelled"


def test_order_ids_are_not_reused(client):
    first_response = client.post(
        "/orders",
        json={
            "user_id": 6,
            "total_amount": "1000.00",
        },
    )

    first_order_id = first_response.json()["id"]

    cancel_response = client.patch(
        f"/orders/{first_order_id}/cancel"
    )

    assert cancel_response.status_code == 200

    second_response = client.post(
        "/orders",
        json={
            "user_id": 7,
            "total_amount": "2000.00",
        },
    )

    assert second_response.status_code == 201

    second_order_id = second_response.json()["id"]

    assert second_order_id != first_order_id
    assert second_order_id > first_order_id


def test_create_order_validation(client):
    response = client.post(
        "/orders",
        json={
            "user_id": 0,
            "total_amount": "1000.00",
        },
    )

    assert response.status_code == 422


def test_order_list_pagination(client):
    for user_id in range(10, 15):
        response = client.post(
            "/orders",
            json={
                "user_id": user_id,
                "total_amount": "100.00",
            },
        )

        assert response.status_code == 201

    response = client.get(
        "/orders?page=1&page_size=2"
    )

    assert response.status_code == 200

    data = response.json()

    assert data["page"] == 1
    assert data["page_size"] == 2
    assert len(data["items"]) <= 2
    assert data["total"] >= 5
    assert data["pages"] >= 3