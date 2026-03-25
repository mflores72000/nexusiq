import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.database import SessionLocal
from src.models import Event

logger = logging.getLogger(__name__)

MACHINE_TYPES = {"L", "M", "H"}
RECENT_WINDOW = 100


def compute_twin_state(db: Session, machine_id: str, as_of: datetime | None = None) -> dict[str, Any] | None:
    """
    Calcula el estado del Digital Twin desde el event log.
    Si `as_of` se especifica, hace time-travel (solo eventos hasta ese timestamp).
    """
    if machine_id not in MACHINE_TYPES:
        return None

    query = """
        SELECT attributes, timestamp, case_id
        FROM events
        WHERE domain = 'SOURCE'
          AND entity_type = 'machine'
          AND entity_id = :machine_id
          {time_filter}
        ORDER BY timestamp ASC
    """
    params: dict = {"machine_id": machine_id}
    if as_of is not None:
        rows = db.execute(
            text(query.format(time_filter="AND timestamp <= :as_of")),
            {**params, "as_of": as_of},
        ).fetchall()
    else:
        rows = db.execute(
            text(query.format(time_filter="")),
            params,
        ).fetchall()

    if not rows:
        return None

    total = len(rows)
    failures = 0
    tool_wears, air_temps, process_temps = [], [], []
    last_ts = None

    for row in rows:
        attrs = row.attributes or {}
        last_ts = row.timestamp
        if attrs.get("Machine failure") == 1:
            failures += 1
        if (tw := attrs.get("Tool wear [min]")) is not None:
            tool_wears.append(float(tw))
        if (at := attrs.get("Air temperature [K]")) is not None:
            air_temps.append(float(at))
        if (pt := attrs.get("Process temperature [K]")) is not None:
            process_temps.append(float(pt))

    recent = rows[-RECENT_WINDOW:]
    recent_failure_rate = sum(1 for r in recent if (r.attributes or {}).get("Machine failure") == 1) / len(recent)

    avg_tool_wear = sum(tool_wears) / len(tool_wears) if tool_wears else 0.0
    avg_air_temp = sum(air_temps) / len(air_temps) if air_temps else 0.0
    avg_process_temp = sum(process_temps) / len(process_temps) if process_temps else 0.0

    # health_score: penaliza falla reciente (60%) y desgaste normalizado (40%)
    MAX_WEAR = 240.0
    health_score = max(0.0, 1.0 - (recent_failure_rate * 0.6) - (min(avg_tool_wear / MAX_WEAR, 1.0) * 0.4))

    return {
        "machine_id": machine_id,
        "pieces_processed": total,
        "total_failures": failures,
        "failure_rate_overall": failures / total if total > 0 else 0.0,
        "failure_rate_recent": recent_failure_rate,
        "avg_tool_wear_min": round(avg_tool_wear, 2),
        "avg_air_temp_k": round(avg_air_temp, 2),
        "avg_process_temp_k": round(avg_process_temp, 2),
        "health_score": round(health_score, 4),
        "last_event_at": last_ts.isoformat() if last_ts else None,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "as_of": as_of.isoformat() if as_of else None,
    }


def materialize_twins(db: Session) -> int:
    """Genera eventos TWIN_STATE para cada máquina. Llamar tras cada ingesta."""
    count = 0
    now = datetime.now(timezone.utc)

    for machine_id in MACHINE_TYPES:
        state = compute_twin_state(db, machine_id)
        if state is None:
            continue

        last_twin = db.execute(
            text("""
                SELECT current_state FROM events
                WHERE domain = 'TWIN_STATE' AND entity_type = 'machine' AND entity_id = :mid
                ORDER BY timestamp DESC LIMIT 1
            """),
            {"mid": machine_id},
        ).fetchone()

        db.add(Event(
            event_id=uuid.uuid4(),
            domain="TWIN_STATE",
            entity_type="machine",
            entity_id=machine_id,
            activity="twin_state_updated",
            timestamp=now,
            attributes={"trigger": "post_ingestion"},
            previous_state=last_twin.current_state if last_twin else None,
            current_state=state,
            data_quality=1.0,
        ))
        db.commit()
        count += 1
        logger.info("Twin materializado: machine=%s health=%.2f", machine_id, state["health_score"])

    return count