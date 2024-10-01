from sqlalchemy import create_engine
from decouple import config
import pandas as pd

# Specify the driver in the connection string
engine = create_engine(
    f"postgresql+psycopg2://{config('DB_USER')}:{config('DB_PASSWORD')}@"
    f"{config('DB_HOST')}:{config('DB_PORT')}/{config('DB_DB')}"
)

df = pd.read_csv("/home/ana/workshop2/Data/the_grammy_awards.csv")

# Pass the engine directly and specify method='multi' (optional)
df.to_sql(name='grammys', con=engine, if_exists='replace', index=False, method='multi')
