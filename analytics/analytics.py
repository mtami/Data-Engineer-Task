import os
import asyncio
import datetime
import time
import pandas as pd

from utils import (
    get_or_create_checkpoint,
    connect_to_source,
    connect_to_analytic_db,
    update_checkpoint
)
from sql_queries import device_agg_hourly_query


print('Waiting for the data generator...')
time.sleep(20)
print('ETL Starting...')


async def etl(source_engin, dest_engin):
    while True:
        try:
            now = datetime.datetime.utcnow()
            this_hour = datetime.datetime(now.year, now.month, now.day, now.hour)
            prev_hour = this_hour - datetime.timedelta(hours=1)
            checkpoint = get_or_create_checkpoint(source_engin)
            if checkpoint:
                before, after = min(checkpoint, prev_hour), max(checkpoint, prev_hour)
                condition = f" where date_hour between '{before}' and '{after}' "
                if checkpoint == this_hour:
                    print("sleep for 1 hour")
                    await asyncio.sleep(60*60)
                    continue
            else:  # first time we run the ETL
                condition = f" where date_hour <= '{prev_hour.strftime('%Y-%m-%d %H:%M:%S')}'"

            query = device_agg_hourly_query.format(condition=condition)

            print("reading aggregations from source ...")
            df = pd.DataFrame(source_engin.execute(query).fetchall())

            print("writing aggregations to analytic db ...")
            df.to_sql(name='analytics', con=dest_engin, if_exists='append', index=False)

            update_checkpoint(source_engin, checkpoint, this_hour)
            await asyncio.sleep(5)
        except Exception as e:
            print(f"ETL Error: {e}")
            await asyncio.sleep(10)


async def main():
    source_engine = connect_to_source(os.environ["POSTGRESQL_CS"])
    dest_engine = connect_to_analytic_db(os.environ["MYSQL_CS"])
    await etl(source_engine, dest_engine)


if __name__ == '__main__':
    asyncio.run(main())
