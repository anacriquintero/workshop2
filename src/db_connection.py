from sqlalchemy import create_engine
from decouple import config

engine = create_engine(
    f"postgresql+psycopg2://{config('DB_USER')}:{config('DB_PASSWORD')}@"
    f"{config('DB_HOST')}:{config('DB_PORT')}/{config('DB_DB')}"
)
def conn():
    return engine