CREATE TABLE velibity.trips (

  id SERIAL,
  insert_datetime TIMESTAMP DEFAULT current_timestamp,
  username VARCHAR(50) NOT NULL,
  start_datetime TIMESTAMP(0) NOT NULL,
  distance_km FLOAT NOT NULL,
  duration_s INT NOT NULL,
  is_elec BOOL NOT NULL

);
