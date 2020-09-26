CREATE TABLE rt_bus_operating_schedule_4_org (
    run_date DATE NOT NULL,
    bus_company VARCHAR(50) NOT NULL,
    bus_team VARCHAR(50),
    peak_period VARCHAR(5),
    departure_bus_times_cnt BIGINT,
    departure_bus_cnt BIGINT,
    running_mileage_sum DECIMAL(9,2),
    PRIMARY KEY (run_date, bus_company, bus_team, peak_period)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE rt_bus_operating_passengers_4_org (
    run_date DATE NOT NULL,
    bus_company VARCHAR(50) NOT NULL,
    bus_team VARCHAR(50),
    peak_period VARCHAR(5),
    passenger_volume_cnt BIGINT,
    passenger_revenue_sum DECIMAL(9,2),
    actual_passenger_revenue_sum DECIMAL(9,2),
    PRIMARY KEY (run_date, bus_company, bus_team, peak_period)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE rt_bus_operating_schedule_4_line (
    run_date DATE NOT NULL,
    line_name VARCHAR(20) NOT NULL,
    direction TINYINT,
    run_hour INT,
    departure_bus_times_cnt BIGINT,
    departure_bus_cnt BIGINT,
    running_mileage_sum DECIMAL(9,2),
    PRIMARY KEY (run_date, line_name, direction, run_hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE rt_bus_operating_passengers_4_line (
    run_date DATE NOT NULL,
    line_name VARCHAR(20) NOT NULL,
    direction TINYINT,
    run_hour INT,
    passenger_volume_cnt BIGINT,
    passenger_revenue_sum DECIMAL(9,2),
    actual_passenger_revenue_sum DECIMAL(9,2),
    PRIMARY KEY (run_date, line_name, direction, run_hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE temporal_bus_operating_schedule (
    id VARCHAR(80),
    bus_id VARCHAR(32),
    bus_company VARCHAR(50),
    bus_team VARCHAR(50),
    peak_period VARCHAR(5),
    line_name VARCHAR(20),
    direction TINYINT,
    run_date DATE,
    actual_start_time TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE index idx_1 ON temporal_bus_operating_schedule (bus_id, run_date);

CREATE TABLE rt_abnormal_departure_interval (
    id VARCHAR(80),
    line_version_id VARCHAR(80),
    bus_company VARCHAR(50),
    bus_team VARCHAR(50),
    line_name VARCHAR(20),
    direction TINYINT,
    run_date DATE,
    plan_departure VARCHAR(6),
    actual_departure VARCHAR(6),
    check_interval INT,
    abnormal_departure INT,
    prev_log VARCHAR(300),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;