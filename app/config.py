from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_hostname: str
    database_port: str
    database_name: str
    database_user: str
    database_password: str
    email_address: str
    email_password: str
    homescore_predictor: str
    awayscore_predictor: str
    outcome_predictor: str


    class Config:
        env_file = ".env"

settings = Settings()