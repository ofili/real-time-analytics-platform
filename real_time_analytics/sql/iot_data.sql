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
)

SELECT create_hypertable('metrics', by_range('time', INTERVAL '1 day'))
    CREATE_DEFAULT false;

CREATE UNIQUE INDEX idx_deviceid_time
  ON metrics(device_id, time DESC);

ALTER TABLE metrics
SET (
	timescaledb.compress,
	timescaledb.compress_segmentby='device_id',
	timescaledb.compress_orderby='time'
);

