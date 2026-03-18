from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from src.database import get_db
from src.twins.twin_service import compute_twin_state, MACHINE_TYPES

router = APIRouter(prefix="/twins", tags=["Digital Twins"])


@router.get("/")
def list_twins(db: Session = Depends(get_db)):
    return {mid: state for mid in MACHINE_TYPES if (state := compute_twin_state(db, mid))}


@router.get("/{machine_id}")
def get_twin(machine_id: str, db: Session = Depends(get_db)):
    machine_id = machine_id.upper()
    if machine_id not in MACHINE_TYPES:
        raise HTTPException(status_code=400, detail=f"machine_id debe ser uno de: {list(MACHINE_TYPES)}")
    state = compute_twin_state(db, machine_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"No hay eventos para la máquina '{machine_id}'.")
    return state


@router.get("/{machine_id}/at")
def get_twin_at(
    machine_id: str,
    timestamp: datetime = Query(..., description="ISO8601 para time-travel"),
    db: Session = Depends(get_db),
):
    """Reconstruye el estado de la máquina en un momento histórico."""
    machine_id = machine_id.upper()
    if machine_id not in MACHINE_TYPES:
        raise HTTPException(status_code=400, detail=f"machine_id debe ser uno de: {list(MACHINE_TYPES)}")
    state = compute_twin_state(db, machine_id, as_of=timestamp)
    if state is None:
        raise HTTPException(status_code=404, detail=f"No hay datos para '{machine_id}' hasta {timestamp.isoformat()}")
    return state
