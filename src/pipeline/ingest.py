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

    logger.info("=" * 70)
    logger.info("🚀 PIPELINE INICIADO")
    logger.info("   job_id : %s", job_id)
    logger.info("   csv    : %s", csv_path)
    logger.info("   hora   : %s", started_at.isoformat())
    logger.info("=" * 70)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV no encontrado: {csv_path}")

    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    stats = {
        "job_id": str(job_id),
        "total_rows": 0,
        "inserted": 0,
        "skipped_duplicates": 0,
        "errors": [],
        "low_quality_rows": 0,
        "by_machine": {"L": 0, "M": 0, "H": 0},
    }

    try:
        logger.info("📋 PASO 1/4 — Emitiendo evento pipeline_started al event log...")
        _emit_system_event(db, "pipeline_started", {"csv_path": csv_path, "started_at": started_at.isoformat()}, job_id)
        logger.info("   ✓ Evento SYSTEM:pipeline_started guardado en BD")

        logger.info("📂 PASO 2/4 — Leyendo CSV y construyendo eventos SOURCE...")
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            batch: list[dict] = []
            BATCH_SIZE = 500
            t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
            batch_num = 0

            for raw_row in reader:
                stats["total_rows"] += 1
                udi = raw_row.get("UDI", "").strip()
                product_id = raw_row.get("Product ID", "").strip()
                machine_type = raw_row.get("Type", "").strip()

                cleaned_attrs, quality = _clean_row(raw_row)

                if quality < 1.0:
                    stats["low_quality_rows"] += 1
                    logger.debug("   ⚠ Fila UDI=%s calidad=%.2f — campos inválidos detectados", udi, quality)

                if machine_type in stats["by_machine"]:
                    stats["by_machine"][machine_type] += 1

                try:
                    ts = t0 + timedelta(minutes=int(udi))
                except (ValueError, TypeError):
                    ts = datetime.now(timezone.utc)
                    logger.warning("   ⚠ UDI no numérico: '%s' — usando timestamp actual", udi)

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
                    batch_num += 1
                    ins, skipped = _bulk_insert(db, batch)
                    stats["inserted"] += ins
                    stats["skipped_duplicates"] += skipped
                    batch = []
                    logger.info(
                        "   📦 Batch #%d | filas=%d | insertadas=%d | duplicados=%d | calidad_baja=%d",
                        batch_num, stats["total_rows"], stats["inserted"],
                        stats["skipped_duplicates"], stats["low_quality_rows"],
                    )

            if batch:
                batch_num += 1
                ins, skipped = _bulk_insert(db, batch)
                stats["inserted"] += ins
                stats["skipped_duplicates"] += skipped
                logger.info("   📦 Batch #%d (final) | insertadas=%d | duplicados=%d", batch_num, ins, skipped)

        logger.info("   ✓ CSV completado — %d batchs procesados", batch_num)
        logger.info("")
        logger.info("📊 PASO 3/4 — Resumen de ingesta:")
        logger.info("   Total filas CSV     : %d", stats["total_rows"])
        logger.info("   Eventos SOURCE insertados  : %d  ← nuevos, UUID5 únicos", stats["inserted"])
        logger.info("   Duplicados saltados : %d  ← ON CONFLICT DO NOTHING (evento ya existía)", stats["skipped_duplicates"])
        logger.info("   Filas calidad <1.0  : %d", stats["low_quality_rows"])
        logger.info("   Breakdown máquinas  : L=%d  M=%d  H=%d",
                    stats["by_machine"]["L"], stats["by_machine"]["M"], stats["by_machine"]["H"])
        logger.info("")
        logger.info("ℹ️  NOTA — Eventos no-SOURCE que se agregan SIEMPRE (cada ejecución):")
        logger.info("   +2 SYSTEM   : pipeline_started + pipeline_completed  (UUID4, siempre nuevos)")
        logger.info("   +3 TWIN_STATE: uno por máquina L/M/H                 (UUID4, siempre nuevos)")
        logger.info("   +2 INSIGHT  : correlaciones calculadas                (UUID4, siempre nuevos)")
        logger.info("   = +7 eventos extra por cada ejecución del pipeline")
        logger.info("   Comportamiento esperado: 1ª vez → 10007, 2ª → 10014, 3ª → 10021...")

        finished_at = datetime.now(timezone.utc)
        stats["duration_seconds"] = (finished_at - started_at).total_seconds()

        logger.info("")
        logger.info("💾 PASO 4/4 — Emitiendo evento pipeline_completed al event log...")
        _emit_system_event(db, "pipeline_completed", {**stats, "finished_at": finished_at.isoformat()}, job_id)
        logger.info("   ✓ Evento SYSTEM:pipeline_completed guardado en BD")

        logger.info("")
        logger.info("=" * 70)
        logger.info("✅ PIPELINE COMPLETADO en %.2fs", stats["duration_seconds"])
        logger.info("=" * 70)

    except Exception as exc:
        logger.error("=" * 70)
        logger.exception("❌ PIPELINE ERROR: %s", exc)
        logger.error("=" * 70)
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


