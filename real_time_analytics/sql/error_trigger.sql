CREATE OR REPLACE FUNCTION record_error()
    RETURNS TRIGGER AS $record_error$
BEGIN
    IF NEW.temperature >= 1000 OR NEW.humidity >= 1000 THEN
        INSERT INTO error_conditions
            VALUES (
                NEW.device_id,
                NEW.location,
                NEW.time,
                NEW.temperature,
                NEW.humidity,
                NEW.pressure,
                NEW.light,
                NEW.sound,
                NEW.motion,
                NEW.vibration,
                NEW.CO2,
                NEW.battery,
                NEW.status
            );
    END IF;
    RETURN NEW;
END;
$record_error$ LANGUAGE plpgsql;

CREATE TRIGGER record_error
    BEFORE INSERT ON iot_data
    FOR EACH ROW
    EXECUTE PROCEDURE record_error();