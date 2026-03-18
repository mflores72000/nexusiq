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


@pytest.fixture(scope="function")
def test_db():
    from src.database import SessionLocal
    db = SessionLocal()
    yield db
    db.close()

def test_time_travel_returns_historical_state(test_db):
    from src.twins.twin_service import MACHINE_TYPES
    db = test_db
    t0 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    machine_id = f"TEST_L_{uuid.uuid4().hex[:6]}"
    MACHINE_TYPES.add(machine_id)
    try:
        for i in range(10):
            _insert_source_event(db, machine_id, str(29000 + i), timestamp=t0 + timedelta(minutes=i), failure=0, tool_wear=i * 10)
        for i in range(10, 20):
            _insert_source_event(db, machine_id, str(29000 + i), timestamp=t0 + timedelta(minutes=i), failure=1, tool_wear=200)

        current = compute_twin_state(db, machine_id)
        assert current is not None
        assert current["pieces_processed"] >= 20

        historical = compute_twin_state(db, machine_id, as_of=t0 + timedelta(minutes=5))
        assert historical is not None
        assert historical["pieces_processed"] <= 10
        assert historical["failure_rate_recent"] == 0.0

    finally:
        db.close()


def test_twin_handles_same_timestamp_events(test_db):
    from src.twins.twin_service import MACHINE_TYPES
    db = test_db
    machine_id = f"TEST_H_{uuid.uuid4().hex[:6]}"
    MACHINE_TYPES.add(machine_id)
    try:
        same_ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        _insert_source_event(db, machine_id, "28001", timestamp=same_ts, failure=0)
        _insert_source_event(db, machine_id, "28002", timestamp=same_ts, failure=1)

        state = compute_twin_state(db, machine_id)
        assert state is not None
        assert state["pieces_processed"] >= 2
    finally:
        db.close()


def test_health_score_range(test_db):
    from src.twins.twin_service import MACHINE_TYPES
    db = test_db
    machine_id = f"TEST_M_{uuid.uuid4().hex[:6]}"
    MACHINE_TYPES.add(machine_id)
    try:
        t = datetime(2024, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
        for i in range(5):
            _insert_source_event(db, machine_id, str(27000 + i), timestamp=t + timedelta(minutes=i), failure=1, tool_wear=230)
        state = compute_twin_state(db, machine_id)
        assert state is not None
        assert 0.0 <= state["health_score"] <= 1.0
    finally:
        db.close()
