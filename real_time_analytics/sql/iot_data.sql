CREATE TABLE IF NOT EXISTS s3_data (
    device_id STRING,
    timestamp BIGINT,
    data STRING
)
USING parquet
LOCATION 's3a://iot-data/';

SELECT create_hypertable('s3_data', by_range('time'));

CREATE TABLE IF NOT EXISTS locations (
    id INTEGER PRIMARY KEY,
    name STRING
);

INSERT INTO locations (id, name) VALUES (1, 'home');
INSERT INTO locations (id, name) VALUES (2, 'office');
INSERT INTO locations (id, name) VALUES (3, 'work');
INSERT INTO locations (id, name) VALUES (4, 'school');
INSERT INTO locations (id, name) VALUES (5, 'gym');
INSERT INTO locations (id, name) VALUES (6, 'car');


CREATE TABLE IF NOT EXISTS iot_data (
    device_id INTEGER CHECK (device_id > 0),
    location INTEGER REFERENCES locations(id),
    time TIMESTAMPTZ,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    light DOUBLE PRECISION,
    sound DOUBLE PRECISION,
    motion DOUBLE PRECISION,
    vibration DOUBLE PRECISION,
    CO2 DOUBLE PRECISION,
    battery DOUBLE PRECISION,
    status INTEGER CHECK (status > 0),
    PRIMARY KEY (time, device_id)
)

SELECT create_hypertable('iot_data', by_range('time', INTERVAL '1 day'))
    CREATE_DEFAULT false;

CREATE UNIQUE INDEX idx_deviceid_time
  ON iot_data(device_id, time DESC);

CREATE TABLE IF NOT EXISTS iot_data_processed (
    device_id STRING,
    timestamp BIGINT,
    data STRING
)
USING parquet
LOCATION 's3a://processed-data/';

CREATE TABLE IF NOT EXISTS iot_data_processed_agg (
    device_id STRING,
    timestamp BIGINT,
    data STRING
)
USING parquet
LOCATION 's3a://processed-data/';
