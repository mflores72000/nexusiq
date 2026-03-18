"""
Tests del Digital Twin — M2

Cubre:
1. Time-travel: estado en timestamp pasado
2. Dos eventos conflictivos con el mismo timestamp
3. Estado actual vs. estado histórico son distintos
"""
from datetime import datetime, timezone, timedelta
import uuid
import pytest
from sqlalchemy.orm import Session

from src.models import Event
from src.twins.twin_service import compute_twin_state


def _insert_source_event(db: Session, machine_type: str, udi: str,
                          timestamp: datetime, failure: int = 0,
                          tool_wear: int = 50) -> None:
    """Helper: inserta un evento SOURCE con valores controlados."""
    event = Event(
        event_id=uuid.uuid4(),
        domain="SOURCE",
        case_id=udi,
        entity_type="machine",
        entity_id=machine_type,
        activity="sensor_reading",
        timestamp=timestamp,
        attributes={
            "UDI": udi,
            "Type": machine_type,
            "Air temperature [K]": 298.1,
            "Process temperature [K]": 308.6,
            "Rotational speed [rpm]": 1551,
            "Torque [Nm]": 42.8,
            "Tool wear [min]": tool_wear,
            "Machine failure": failure,
            "TWF": 0, "HDF": 0, "PWF": 0, "OSF": 0, "RNF": 0,
        },
        data_quality=1.0,
    )
    db.add(event)
    db.commit()


# ─── TEST 1: Time-travel funciona correctamente ───────────────────────────────
def test_time_travel_returns_historical_state():
    """
    CASO BORDE 4 (parcial) + Requerimiento obligatorio:
    El time-travel debe retornar el estado del twin como era en el pasado.
    """
    from src.database import SessionLocal
    db = SessionLocal()
    try:
        t0 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # Insertar 10 eventos en t0..t0+9min
        for i in range(10):
            _insert_source_event(
                db, "L", str(9000 + i),
                timestamp=t0 + timedelta(minutes=i),
                failure=0,
                tool_wear=i * 10,
            )
        # Insertar 10 eventos más (con fallas) en t0+10min..t0+19min
        for i in range(10, 20):
            _insert_source_event(
                db, "L", str(9000 + i),
                timestamp=t0 + timedelta(minutes=i),
                failure=1,
                tool_wear=200,
            )

        # Estado actual: debe incluir todos (20 eventos, falla rate alta)
        current = compute_twin_state(db, "L")
        assert current is not None
        assert current["pieces_processed"] >= 20

        # Time-travel al minuto 5: solo debe ver los primeros 6 eventos (sin fallas)
        historical = compute_twin_state(db, "L", as_of=t0 + timedelta(minutes=5))
        assert historical is not None
        assert historical["pieces_processed"] <= 10
        assert historical["failure_rate_recent"] == 0.0, \
            "En t=5min no debería haber fallas aún"

    finally:
        db.close()


# ─── TEST 2: Dos eventos con el mismo timestamp ───────────────────────────────
def test_twin_handles_same_timestamp_events():
    """
    CASO BORDE 4: El twin recibe dos eventos con el mismo timestamp.
    No debe lanzar excepción; debe procesar ambos.
    """
    from src.database import SessionLocal
    db = SessionLocal()
    try:
        same_ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

        _insert_source_event(db, "H", "8001", timestamp=same_ts, failure=0)
        _insert_source_event(db, "H", "8002", timestamp=same_ts, failure=1)

        # No debe lanzar excepción
        state = compute_twin_state(db, "H")
        assert state is not None
        assert state["pieces_processed"] >= 2

    finally:
        db.close()


# ─── TEST 3: health_score dentro de rango válido ─────────────────────────────
def test_health_score_range():
    """El health_score debe estar siempre entre 0.0 y 1.0."""
    from src.database import SessionLocal
    db = SessionLocal()
    try:
        t = datetime(2024, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
        for i in range(5):
            _insert_source_event(
                db, "M", str(7000 + i),
                timestamp=t + timedelta(minutes=i),
                failure=1,
                tool_wear=230,
            )
        state = compute_twin_state(db, "M")
        assert state is not None
        assert 0.0 <= state["health_score"] <= 1.0

    finally:
        db.close()
