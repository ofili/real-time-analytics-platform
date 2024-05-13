import psycopg2

from real_time_analytics.core.config import Settings

query_create_table = """
    CREATE TABLE IF NOT EXISTS metrics (
        device_id INTEGER CHECK (device_id > 0),
        location INTEGER REFERENCES locations(id),
        time TIMESTAMPTZ,
        device_type INTEGER REFERENCES device_types(id),
        cpu DOUBLE PRECISION,
        memory DOUBLE PRECISION,
        disk_io DOUBLE PRECISION,
        energy_consumption DOUBLE PRECISION,
        status_code INTEGER,
        temperature DOUBLE PRECISION,
        PRIMARY KEY (time, device_id)
    );
"""
query_create_hypertable = """
    SELECT create_hypertable('metrics', by_range('time', INTERVAL '1 day'))
    CREATE_DEFAULT false;
"""

query_create_index = """
    CREATE UNIQUE INDEX idx_deviceid_time
  ON metrics(device_id, time DESC);
"""

query_alter_table = """
    ALTER TABLE metrics
        SET (
            timescaledb.compress,
            timescaledb.compress_segmentby='device_id',
            timescaledb.compress_orderby='time'
        );
    """

CONNECTION = f"postgres://{Settings.postgres_username}:{Settings.postgres_password}@{Settings.postgres_host}:{Settings.postgres_port}/{Settings.postgres_database}"

with psycopg2.connect(CONNECTION) as conn:
    cur = conn.cursor()
    cur.execute(query_create_table)
    cur.execute(query_create_hypertable)
    cur.execute(query_create_index)
    cur.execute(query_alter_table)
    conn.commit()

    print("Tables created")
    conn.close()
