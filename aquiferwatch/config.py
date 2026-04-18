from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"


class Settings(BaseSettings):
    """Runtime config. Values pulled from .env or environment."""

    model_config = SettingsConfigDict(env_file=REPO_ROOT / ".env", extra="ignore")

    # Shared infra (reused from Agricultural_Data_Analysis)
    database_url: str = Field(default="", alias="DATABASE_URL")
    aws_region: str = Field(default="us-east-2", alias="AWS_REGION")
    s3_bucket: str = Field(default="usda-analysis-datasets", alias="S3_BUCKET")

    # API keys
    quickstats_api_key: str = Field(default="", alias="QUICKSTATS_API_KEY")
    noaa_api_key: str = Field(default="", alias="NOAA_API_KEY")

    # AquiferWatch-specific
    usgs_user_agent: str = Field(
        default="AquiferWatch/0.1", alias="USGS_USER_AGENT"
    )
    kgs_wimas_base_url: str = Field(
        default="https://www.kgs.ku.edu/Magellan/WaterWell/",
        alias="KGS_WIMAS_BASE_URL",
    )
    twdb_base_url: str = Field(
        default="https://www.twdb.texas.gov/groundwater/data/",
        alias="TWDB_BASE_URL",
    )
    ne_dnr_base_url: str = Field(
        default="https://dnr.nebraska.gov/", alias="NE_DNR_BASE_URL"
    )

    # MLflow
    mlflow_tracking_uri: str = Field(
        default="sqlite:///mlflow.db", alias="MLFLOW_TRACKING_URI"
    )
    mlflow_artifact_location: str = Field(
        default="./mlartifacts", alias="MLFLOW_ARTIFACT_LOCATION"
    )

    # Experiment metadata
    team_member: str = Field(default="unknown", alias="AQW_TEAM_MEMBER")


settings = Settings()
