from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg2://nexus:nexus@localhost:5432/nexusiq"
    csv_path: str = "./data/ai4i2020.csv"
    app_name: str = "NexusIQ Platform"
    app_version: str = "1.0.0"

    model_config = {"env_file": ".env", "case_sensitive": False}


settings = Settings()
