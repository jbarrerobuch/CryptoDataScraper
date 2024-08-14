from . import execute_sql
from ..Agent import Collector

__all__ = ["write_instruments_status"]

def write_instruments_status(agent:Collector) -> None:

        # SET EXPIRED INSTRUMENTS TO is_active = False
        sql = "UPDATE instruments SET is_active = false\n\
            WHERE expiration_timestamp < CURRENT_DATE;"
        execute_sql(agent=agent, sql=sql)
        print("Updated expiration statuses")

        # SET COMPLETE INSTRUMENTS TO is_complete = True
        sql = """
            UPDATE instruments
            SET is_complete = TRUE
            FROM (
                SELECT
                    instruments.instrument_name,
                    instruments.creation_timestamp as creation_timestamp,
                    min(timestamp) as min_timestamp,
                    instruments.expiration_timestamp as expiration_timestamp,
                    max(timestamp) as max_timestamp
                FROM market_data
                INNER JOIN instruments
                ON market_data.instrument_id = instruments.instrument_id
                GROUP BY instruments.instrument_name, instruments.creation_timestamp, instruments.expiration_timestamp
                HAVING instruments.expiration_timestamp <= max(market_data.timestamp)
            ) AS subquery
            WHERE instruments.instrument_name = subquery.instrument_name;
        """
        execute_sql(agent=agent, sql=sql)
        print("Updated complete statuses")