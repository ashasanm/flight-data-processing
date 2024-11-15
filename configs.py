from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List


class DatabaseSettings(BaseModel):
    POSTGRES_DB: str = Field(None)
    POSTGRES_USER: str = Field(None)
    POSTGRES_PASSWORD: str = Field(None)
    POSTGRES_HOST: str = Field(None)
    POSTGRES_PORT: int = Field(5432)


class DjangoSettings(BaseModel):
    SECRET_KEY: str = Field(None)
    DEBUG: bool = Field(True)
    ALLOWED_HOSTS: str = Field(["localhost", "127.0.0.1", "*"])

    @field_validator("ALLOWED_HOSTS", mode="after")
    def parse_allowed_hosts(cls, v):
        if isinstance(v, str):
            return [host.strip() for host in v.split(",")]
        return v


class RedisSettings(BaseModel):
    HOST: str = Field(None)
    PORT: int = Field(6379)


class SparkSettings(BaseModel):
    MODE: str = Field("master")
    UI_PORT: int = Field(8080)
    EXECUTOR_MEMORY: str = Field("4g")
    DRIVER_MEMORY: str = Field("4g")
    EXECUTOR_CORES: int = Field(2)
    NUM_EXECUTORS: int = Field(10)
    DYNAMIC_ALLOCATION_ENABLED: bool = Field(True)
    DYNAMIC_ALLOCATION_MIN_EXECUTORS: int = Field(1)
    DYNAMIC_ALLOCATION_MAX_EXECUTORS: int = Field(50)
    SQL_SHUFFLE_PARTITIONS: int = Field(200)
    SQL_ADAPTIVE_ENABLED: bool = Field(True)
    SQL_ADAPTIVE_SHUFFLE_TARGET_SIZE: int = Field(134217728)
    BAD_RECORDS_PATH: str = Field("/flight_data/spark/records")


class CelerySettings(BaseModel):
    BROKER_URL: str = Field(None)
    RESULT_BACKEND: str = Field(None)


class AppConfig(BaseSettings):
    database: DatabaseSettings
    django: DjangoSettings
    redis: RedisSettings
    spark: SparkSettings
    celery: CelerySettings

    # Allow extra fields in the environment
    class Config:
        env_nested_delimiter = "__"
        env_file = ".env"


# Load the settings from the environment or .env file
config = AppConfig()
