CREATE TABLE IF NOT EXISTS locations (
    id INTEGER PRIMARY KEY,
    name TEXT
);

INSERT INTO locations (id, name) VALUES (1, 'home');
INSERT INTO locations (id, name) VALUES (2, 'office');
INSERT INTO locations (id, name) VALUES (3, 'work');
INSERT INTO locations (id, name) VALUES (4, 'school');
INSERT INTO locations (id, name) VALUES (5, 'gym');
INSERT INTO locations (id, name) VALUES (6, 'car');


CREATE TABLE IF NOT EXISTS device_types( id INTEGER PRIMARY KEY,
    name TEXT);

INSERT INTO device_types (id, name) VALUES (1, 'phone');
INSERT INTO device_types (id, name) VALUES (2, 'laptop');
INSERT INTO device_types (id, name) VALUES (3, 'tablet');
INSERT INTO device_types (id, name) VALUES (4, 'desktop');
INSERT INTO device_types (id, name) VALUES (5, 'tv');
INSERT INTO device_types (id, name) VALUES (6, 'car');

CREATE TABLE IF NOT EXISTS devices (
    id INTEGER PRIMARY KEY,
    name TEXT,
    location_id INTEGER,
    device_type_id INTEGER
);

INSERT INTO devices (id, name, location_id, device_type_id) VALUES (1, 'phone', 1, 1);
INSERT INTO devices (id, name, location_id, device_type_id) VALUES (2, 'laptop', 2, 2);
INSERT INTO devices (id, name, location_id, device_type_id) VALUES (3, 'tablet', 3, 3);
INSERT INTO devices (id, name, location_id, device_type_id) VALUES (4, 'desktop', 4, 4);
INSERT INTO devices (id, name, location_id, device_type_id) VALUES (5, 'tv', 5, 5);
INSERT INTO devices (id, name, location_id, device_type_id) VALUES (6, 'car', 6, 6);

