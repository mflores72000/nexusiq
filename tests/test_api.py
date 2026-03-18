"""
Tests de la REST API — M4

Tests de integración para los endpoints principales.
"""
import pytest
from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)


def test_root_endpoint():
    """GET / debe responder con la info de la app."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "endpoints" in data
    assert "/health" in str(data["endpoints"])


def test_health_endpoint_structure():
    """GET /health debe retornar los campos requeridos por el enunciado."""
    response = client.get("/health/stats")
    assert response.status_code == 200
    data = response.json()

    required_fields = ["status", "event_count", "avg_lag_ms", "stale_twins", "last_run_at"]
    for field in required_fields:
        assert field in data, f"Campo requerido faltante: {field}"

    assert data["status"] in ("healthy", "empty")
    assert isinstance(data["event_count"], int)
    assert isinstance(data["avg_lag_ms"], (int, float))


def test_events_endpoint_pagination():
    """GET /events debe soportar paginación con limit y offset."""
    response = client.get("/events?limit=10&offset=0")
    assert response.status_code == 200
    data = response.json()

    assert "total" in data
    assert "limit" in data
    assert "items" in data
    assert isinstance(data["items"], list)
    assert len(data["items"]) <= 10


def test_events_filter_by_domain():
    """GET /events?domain=SOURCE debe filtrar correctamente."""
    response = client.get("/events?domain=SOURCE&limit=5")
    assert response.status_code == 200
    data = response.json()
    for item in data["items"]:
        assert item["domain"] == "SOURCE"


def test_twins_invalid_machine_id():
    """GET /twins/X (ID inválido) debe retornar 400."""
    response = client.get("/twins/X")
    assert response.status_code == 400


def test_correlations_endpoint():
    """GET /correlations debe retornar estructura correcta."""
    response = client.get("/correlations")
    assert response.status_code == 200
    data = response.json()
    assert "correlations" in data
    assert isinstance(data["correlations"], list)
