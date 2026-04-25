from pydantic import BaseModel
from pydantic_settings import BaseSettings


class AppConfig(BaseModel):
    title: str = 'Reporting Cloud'
    summary: str = 'Reporting Cloud'
    description: str = '"Watts" Up with Our Plants?'
    version: str = '0.9.3'
    contact: dict[str, str] = {
        'name': 'Christopher Klix',
        'url': 'https://github.com/ChristopherKlix',
        'email': 'christopher.klix@googlemail.com'
    }


class MaStRConfig(BaseModel):
    marktakteur_mastr_nummer: str
    secret: str


class DigitalizationConfig(BaseModel):
    api_url: str
    api_key: str


class DBConfig(BaseModel):
    host: str
    port: int = 5432
    name: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class Settings(BaseSettings):
    app: AppConfig = AppConfig()
    db: DBConfig
    mastr: MaStRConfig
    digitalization: DigitalizationConfig

    class Config:
        env_nested_delimiter = '__'
        env_file = '.env'
        env_file_encoding = 'utf-8'


settings = Settings()
