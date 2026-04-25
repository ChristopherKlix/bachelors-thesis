"""
mastr.py
========

"""

# Standard library
import asyncio

# Third-party
import pandas as pd
import psycopg2
from psycopg2 import sql

from settings import settings


class MaStR:
    """
    
    """

    def __init__(self):
        self._conn = self._get_db_connection()

    async def get(self) -> pd.DataFrame:
        sql_query: sql.Composable = sql.SQL('''
            SELECT
                see.*
            FROM main_data_2 md2
            LEFT JOIN mastr_einheitensolar see
            ON md2.see_number = see.see_number
            WHERE md2.see_number LIKE 'SEE%'
            ORDER BY see.registration_date ASC
        ''')

        with self._conn.cursor() as cur:
            cur.execute(sql_query)

        def run_query() -> pd.DataFrame:
            with self._conn.cursor() as cur:
                cur.execute(sql_query)
                cols = [desc[0] for desc in cur.description]
                df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

                return df
            
        df: pd.DataFrame = await asyncio.to_thread(run_query)

        return df

    ################################################################
    # Internal methods - db utility
    ################################################################

    def _get_db_connection(self):
        conn = psycopg2.connect(settings.db.dsn)
    
        return conn
