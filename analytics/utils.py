from datetime import datetime
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine.base import Engine
from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime, Float


def get_or_create_checkpoint(engine) -> datetime:
    while True:
        try:
            metadata_obj = MetaData()
            checkpoints = Table(
                'checkpoints', metadata_obj,
                Column('last_checkpoint', DateTime),
            )
            metadata_obj.create_all(engine)
            break
        except OperationalError:
            time.sleep(0.1)

    checkpoint = engine.execute("SELECT last_checkpoint FROM checkpoints").fetchone()
    return checkpoint[0] if checkpoint else None


def update_checkpoint(engine, old_checkpoint, new_checkpoint):
    print(f"update checkpoint to {new_checkpoint}")
    if not old_checkpoint:
        engine.execute(f"insert into checkpoints (last_checkpoint) values ('{new_checkpoint}')")
    else:
        engine.execute(f"UPDATE checkpoints SET last_checkpoint = '{new_checkpoint}' WHERE last_checkpoint= '{old_checkpoint}'")


def connect_to_source(conn) -> Engine:
    while True:
        try:
            psql_engine = create_engine(conn)
            break
        except OperationalError:
            time.sleep(0.1)
    print('Connection to PostgresSQL successful.')
    return psql_engine


def connect_to_analytic_db(conn) -> Engine:
    while True:
        try:
            mysql_engine = create_engine(conn)
            metadata_obj = MetaData()
            analytics = Table(
                'analytics', metadata_obj,
                Column('device_id', String(64)),
                Column('date_hour', DateTime),
                Column('max_temperature', Integer),
                Column('total_measurements', Integer),
                Column('distance_km', Float),
            )
            metadata_obj.create_all(mysql_engine)
            break
        except OperationalError as e:
            print(e)
            time.sleep(0.1)
    print('Connection to Analytic DB successful.')
    return mysql_engine
