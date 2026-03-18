import csv
import hashlib
import logging
import uuid
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.config import settings
from src.database import SessionLocal
from src.models import Event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger(__name__)

_UUID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
_INVALID_NUMERIC_VALUES = {"n/a", "na", "null", "none", "--", "-", "", "nan"}
_NUMERIC_COLS = {
    "Air temperature [K]", "Process temperature [K]", "Rotational speed [rpm]",
    "Torque [Nm]", "Tool wear [min]", "Machine failure",
    "TWF", "HDF", "PWF", "OSF", "RNF",
}


def _clean_row(row: dict[str, str]) -> tuple[dict[str, Any], float]:
    """Limpia una fila del CSV. Retorna (attributes, data_quality 0-1)."""
    cleaned: dict[str, Any] = {}
    total_fields = valid_fields = 0

    for key, raw_value in row.items():
        total_fields += 1
        stripped = raw_value.strip()
        if key in _NUMERIC_COLS:
            if stripped.lower() in _INVALID_NUMERIC_VALUES:
                cleaned[key] = None
                logger.warning("Campo inválido: col='%s' valor='%s'", key, raw_value)
            else:
                try:
                    val = float(stripped)
                    cleaned[key] = int(val) if val == int(val) else val
                    valid_fields += 1
                except (ValueError, OverflowError):
                    cleaned[key] = None
        else:
            cleaned[key] = stripped if stripped else None
            if stripped:
                valid_fields += 1

    return cleaned, valid_fields / total_fields if total_fields > 0 else 0.0


def _make_event_id(udi: str, row_content: str) -> uuid.UUID:
    """UUID5 determinístico: mismo UDI + mismo contenido → mismo ID."""
    return uuid.uuid5(_UUID_NAMESPACE, f"SOURCE:{udi}:{row_content}")


def _row_fingerprint(row: dict[str, str]) -> str:
    normalized = "|".join(f"{k}={v.strip()}" for k, v in sorted(row.items()))
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def _emit_system_event(db: Session, activity: str, attributes: dict, job_id: uuid.UUID) -> None:
    db.add(Event(
        event_id=uuid.uuid4(),
        domain="SYSTEM",
        case_id=None,
        entity_type="pipeline",
        entity_id="pipeline-main",
        activity=activity,
        timestamp=datetime.now(timezone.utc),
        attributes={**attributes, "job_id": str(job_id)},
        data_quality=1.0,
    ))
    db.commit()
    logger.info("SYSTEM event: %s", activity)


def _bulk_insert(db: Session, batch: list[dict]) -> tuple[int, int]:
    """INSERT ... ON CONFLICT DO NOTHING. Retorna (insertados, saltados)."""
    if not batch:
        return 0, 0
    stmt = pg_insert(Event).values(batch).on_conflict_do_nothing(index_elements=["event_id"])
    result = db.execute(stmt)
    db.commit()
    inserted = result.rowcount
    return inserted, len(batch) - inserted


def run_ingestion(csv_path: str | None = None, db: Session | None = None) -> dict:
    """Lee el CSV, valida, deduplica por UUID5 y carga al event log."""
    csv_path = csv_path or settings.csv_path
    job_id = uuid.uuid4()
    started_at = datetime.now(timezone.utc)

    logger.info("=" * 60)
    logger.info("Iniciando pipeline — job_id=%s | CSV: %s", job_id, csv_path)
    logger.info("=" * 60)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV no encontrado: {csv_path}")

    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    stats = {"job_id": str(job_id), "total_rows": 0, "inserted": 0, "skipped_duplicates": 0, "errors": []}

    try:
        _emit_system_event(db, "pipeline_started", {"csv_path": csv_path, "started_at": started_at.isoformat()}, job_id)

        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            batch: list[dict] = []
            BATCH_SIZE = 500
            t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)

            for raw_row in reader:
                stats["total_rows"] += 1
                udi = raw_row.get("UDI", "").strip()
                product_id = raw_row.get("Product ID", "").strip()
                machine_type = raw_row.get("Type", "").strip()

                cleaned_attrs, quality = _clean_row(raw_row)

                try:
                    ts = t0 + timedelta(minutes=int(udi))
                except (ValueError, TypeError):
                    ts = datetime.now(timezone.utc)

                event_id = _make_event_id(udi, _row_fingerprint(raw_row))
                batch.append({
                    "event_id": event_id,
                    "domain": "SOURCE",
                    "case_id": udi,
                    "entity_type": "machine",
                    "entity_id": machine_type,
                    "activity": "sensor_reading",
                    "timestamp": ts,
                    "attributes": {**cleaned_attrs, "product_id": product_id},
                    "previous_state": None,
                    "current_state": None,
                    "data_quality": quality,
                })

                if len(batch) >= BATCH_SIZE:
                    ins, skipped = _bulk_insert(db, batch)
                    stats["inserted"] += ins
                    stats["skipped_duplicates"] += skipped
                    batch = []
                    logger.info("Progreso: %d filas procesadas, %d insertadas", stats["total_rows"], stats["inserted"])

            if batch:
                ins, skipped = _bulk_insert(db, batch)
                stats["inserted"] += ins
                stats["skipped_duplicates"] += skipped

        finished_at = datetime.now(timezone.utc)
        stats["duration_seconds"] = (finished_at - started_at).total_seconds()
        _emit_system_event(db, "pipeline_completed", {**stats, "finished_at": finished_at.isoformat()}, job_id)

        logger.info("=" * 60)
        logger.info("✅ Pipeline OK | filas=%d insertadas=%d skip=%d %.2fs",
                    stats["total_rows"], stats["inserted"], stats["skipped_duplicates"], stats["duration_seconds"])
        logger.info("=" * 60)

    except Exception as exc:
        logger.exception("❌ Pipeline error: %s", exc)
        stats["errors"].append(str(exc))
        try:
            _emit_system_event(db, "pipeline_error", {"error": str(exc), **stats}, job_id)
        except Exception:
            pass
        raise
    finally:
        if close_db:
            db.close()

    return stats


if __name__ == "__main__":
    run_ingestion()
