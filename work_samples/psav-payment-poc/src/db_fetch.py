from sqlalchemy import text
import pandas as pd
from src.db_connection import get_engine

QUERY = """
SELECT *
FROM your_table
"""

def fetch_data() -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(QUERY), conn)
