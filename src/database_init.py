import logging
from src.database import engine
from src.models import Base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_db() -> None:
    logger.info("Creando tablas...")
    Base.metadata.create_all(bind=engine)
    logger.info("✅ Tablas creadas.")


if __name__ == "__main__":
    init_db()
