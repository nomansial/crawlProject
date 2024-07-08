import secrets

from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    class Config:
        secrets_dir = "/run/secrets"

    PROJECT_NAME: str = "job-postings-crawler"

    # Set env variable to wherever it's deployed, e.g. "http://127.0.0.1:8000"
    BASE_URL: str = "http://localhost:8000"
    PORT: int = 8000
    API_V1_PREFIX: str = "/api/v1"
    SECRET_KEY: str = secrets.token_urlsafe(32)

    SF_API_URL: str = "https://scraping.narf.ai/api/v1/"
    SF_API_KEY: str = "5B1vJSUe9NRGTQ0oP1AmD66LRRqzZzFMcjyoxHEZANDSQPA4vyljhsfKJzhMMK5uczL3qMSillraTlD5KD"
    # 5B1vJSUe9NRGTQ0oPlAmD66LRRqzZzFMcjyoxHEZANDSQPA4vyljhsfKJzhM MK5uczL3qMSillraTlD5KD
    REQUESTS_CONCURRENCY: int = 3
    URL_MAXIMUM_RETRIES: int = 10

    POSTGRES_DSN: str = (
        "postgres://postgres:abc123@localhost:5432/postgres"
    )   

    MSSQL_DSN: str = "Driver={ODBC Driver 17 for SQL Server};Server=NOMANSIAL\SQLEXPRESS;Database=ActiveM_DemoERP;Trusted_Connection=yes;"
    
    #DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ActiveM_DemoERP;UID=sa;PWD=YourStrong!Passw0rd
    #Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=ActiveM_DemoERP;Trusted_Connection=yes;

    BACKEND_CORS_ORIGINS: list[AnyHttpUrl] = [
        "http://localhost",
        "http://localhost:8000",
    ]


settings = Settings()
