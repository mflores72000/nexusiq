from sqlalchemy import Column, String, Float, DateTime, Index
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import func
import uuid


class Base(DeclarativeBase):
    pass


class Event(Base):
    """
    Única tabla del sistema. Los eventos son inmutables: solo INSERT.
    domain: SOURCE | TWIN_STATE | INSIGHT | SYSTEM
    """
    __tablename__ = "events"

    event_id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    domain = Column(String(20), nullable=False)
    case_id = Column(String(50), nullable=True)
    entity_type = Column(String(20), nullable=False)
    entity_id = Column(String(100), nullable=False)
    activity = Column(String(100), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    ingested_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    attributes = Column(JSONB, nullable=True)
    previous_state = Column(JSONB, nullable=True)
    current_state = Column(JSONB, nullable=True)
    data_quality = Column(Float, nullable=False, default=1.0)

    __table_args__ = (
        Index("ix_events_domain", "domain"),
        Index("ix_events_entity_id", "entity_id"),
        Index("ix_events_entity_type_entity_id", "entity_type", "entity_id"),
        Index("ix_events_timestamp", "timestamp"),
        Index("ix_events_case_id", "case_id"),
        Index("ix_events_domain_entity_id_ts", "domain", "entity_id", "timestamp"),
    )

    def __repr__(self) -> str:
        return f"<Event {self.domain} {self.activity} entity={self.entity_id}>"
