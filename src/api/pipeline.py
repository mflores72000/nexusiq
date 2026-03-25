import uuid
import logging
from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.database import get_db, SessionLocal
from src.pipeline.ingest import run_ingestion
from src.twins.twin_service import materialize_twins
from src.correlations.engine import run_correlation_engine

logger = logging.getLogger(__name__)

router_correlations = APIRouter(prefix="/correlations", tags=["Correlations"])
router_pipeline = APIRouter(prefix="/pipeline", tags=["Pipeline"])
router_admin = APIRouter(prefix="/admin", tags=["Admin"])


@router_correlations.get("/")
def get_correlations(db: Session = Depends(get_db)):
    rows = db.execute(
        text("SELECT event_id, timestamp, attributes FROM events WHERE domain = 'INSIGHT' AND activity = 'correlation_found' ORDER BY timestamp DESC")
    ).fetchall()

    if not rows:
        return {"message": "Sin correlaciones aún. Ejecutar POST /pipeline/run primero.", "correlations": []}

    return {
        "total": len(rows),
        "correlations": [
            {"event_id": str(r.event_id), "computed_at": r.timestamp.isoformat() if r.timestamp else None, **r.attributes}
            for r in rows
        ],
    }


@router_correlations.post("/run")
def run_correlations_now(db: Session = Depends(get_db)):
    results = run_correlation_engine(db)
    return {"message": f"{len(results)} correlaciones significativas encontradas.", "count": len(results)}


@router_pipeline.post("/run")
def trigger_pipeline(background_tasks: BackgroundTasks):
    """Dispara la ingesta en background. Ver progreso en GET /events?domain=SYSTEM."""
    job_id = str(uuid.uuid4())

    def _run():
        log = logging.getLogger(__name__)
        try:
            run_ingestion()
            db = SessionLocal()
            try:
                materialize_twins(db)
                run_correlation_engine(db)
            finally:
                db.close()
        except Exception as exc:
            log.exception("Pipeline job %s falló: %s", job_id, exc)

    background_tasks.add_task(_run)
    return {"job_id": job_id, "status": "started", "message": "Ver progreso en GET /events?domain=SYSTEM"}


@router_admin.delete("/reset")
def reset_database(db: Session = Depends(get_db)):
    """⚠️ Elimina TODOS los eventos. Útil para demos. Reseta el sistema a cero."""
    count = db.execute(text("SELECT COUNT(*) FROM events")).scalar() or 0
    db.execute(text("DELETE FROM events"))
    db.commit()
    logger.warning("🗑️  RESET: Se eliminaron %d eventos de la BD.", count)
    return {
        "status": "reset_ok",
        "deleted_events": count,
        "message": f"Se eliminaron {count} eventos. Ejecuta POST /pipeline/run para reingestar.",
    }

