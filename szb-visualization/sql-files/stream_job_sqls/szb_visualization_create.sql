@stream_sql{jobName=szb_visualization_create,dependent_job=true};


------------------------------------------------------------------------------
-- 线路基础信息(维表)
CREATE TABLE tbl_base_line (
    line_version_id VARCHAR(80),
    line_name VARCHAR(20),
    line_status TINYINT,
    org_id varchar(50)
) WITH (
    'table-name' = 'tbl_base_line',
    t{dispatch_slave_base_data_mysql_conf}
);

-- 组织信息(维表)
CREATE TABLE tbl_base_organization (
    org_id VARCHAR(50),
    org_name VARCHAR(50),
    org_parent_id VARCHAR(50)
) WITH (
    'table-name' = 'tbl_base_organization',
    t{dispatch_slave_base_data_mysql_conf}
);

-- 线路路径(维表)
CREATE TABLE tbl_base_line_route (
    route_id VARCHAR(50),
    line_version_id VARCHAR(50),
    route_direction TINYINT,
    route_operate_type TINYINT COMMENT '路径营运类型 1营运 2非营运'
) WITH (
    'table-name' = 'tbl_base_line_route',
    t{dispatch_slave_base_data_mysql_conf}
);

-- 线路路径时段信息(维表)
CREATE TABLE tbl_base_route_period (
    route_id VARCHAR(50),
    line_version_id VARCHAR(50),
    period_name VARCHAR(50) COMMENT '时段名称',
    period_peak TINYINT COMMENT '是否高峰时段  0：否 1：是',
    period_start_time VARCHAR(5) COMMENT '开始时间',
    period_end_time VARCHAR(5) COMMENT '结束时间',
    check_interval INT COMMENT '考核间隔,单位(秒)',
    period_type TINYINT COMMENT '时段类型 1：平日2：双休日3：节假日 4：节假日前夕'
) WITH (
    'table-name' = 'tbl_base_route_period',
    t{dispatch_slave_base_data_mysql_conf}
);

-- 公交日历信息(维表)
CREATE TABLE tbl_base_calendar (
    calendar_date DATE,
    calendar_type TINYINT COMMENT '日期类型 1：平日  2：双休日  3：节假日  4：节假日前夕'
) WITH (
    'table-name' = 'tbl_base_calendar',
    t{dispatch_slave_base_data_mysql_conf}
);

-- 车辆信息表(维表)
CREATE TABLE tbl_base_bus (
    bus_plate_number VARCHAR(32) COMMENT '自编车号',
    bus_id VARCHAR(32) COMMENT 'bus主键ID'
) WITH (
    'table-name' = 'tbl_base_bus',
    t{dispatch_slave_base_data_mysql_conf}
);

-- 公交营运班次(结果维表)
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
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table-name' = 'temporal_bus_operating_schedule',
    t{bdnode5_szb_visualization_mysql_conf}
);


------------------------------------------------------------------------------
-- 公交营运班次(主表)
CREATE TABLE tbl_ehualu_trip (
    id VARCHAR(80),
    run_date INT,
    line_version_id VARCHAR(80),
    line_code VARCHAR(20),
    direction TINYINT,
    trip_type INT,
    bus_id VARCHAR(80),
    plan_distance FLOAT,
    record_status INT,
    plan_time_start BIGINT,
    plan_time_end BIGINT,
    real_time_start BIGINT,
    real_time_end BIGINT,
    proc_time AS PROCTIME()
) PARTITIONED BY (line_code, direction) WITH (
    'topic' = 'dispatch-mysql-slave.db_trip_gather.tbl_ehualu_trip',
    'properties.group.id' = 'szb_visualization',
    'format' = 'debezium-json',
    t{ud_kafka_conf}
);

-- 深圳通刷卡数据(主表)
CREATE TABLE szt_pay_card (
    busNo VARCHAR(10),
    txnType TINYINT,
    cardNo VARCHAR(60),
    txnPrice INT,
    txnAmount INT,
    txnDate BIGINT,
    ts AS TO_TIMESTAMP(txnDate),
    proc_time AS PROCTIME(),
    WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
) PARTITIONED BY (cardNo) WITH (
    'properties.group.id' = 'szb_visualization_dev',
    t{dispatch_card_kafka_conf}
);


------------------------------------------------------------------------------
-- 公交营运班次-组织维度(结果表)
CREATE TABLE rt_bus_operating_schedule_4_org (
    run_date DATE,
    bus_company VARCHAR(50),
    bus_team VARCHAR(50),
    peak_period VARCHAR(5),
    departure_bus_times_cnt BIGINT,
    departure_bus_cnt BIGINT,
    running_mileage_sum DECIMAL(9,2),
    PRIMARY KEY (run_date, bus_company, bus_team, peak_period) NOT ENFORCED
) WITH (
    'table-name' = 'rt_bus_operating_schedule_4_org',
    t{bdnode5_szb_visualization_mysql_conf}
);

-- 公交营运载客-组织维度(结果表)
CREATE TABLE rt_bus_operating_passengers_4_org (
    run_date DATE,
    bus_company VARCHAR(50),
    bus_team VARCHAR(50),
    peak_period VARCHAR(5),
    passenger_volume_cnt BIGINT,
    passenger_revenue_sum DECIMAL(9,2),
    actual_passenger_revenue_sum DECIMAL(9,2),
    PRIMARY KEY (run_date, bus_company, bus_team, peak_period) NOT ENFORCED
) WITH (
    'table-name' = 'rt_bus_operating_passengers_4_org',
    t{bdnode5_szb_visualization_mysql_conf}
);

-- 公交营运班次-线路维度(结果表)
CREATE TABLE rt_bus_operating_schedule_4_line (
    run_date DATE,
    line_name VARCHAR(20),
    direction TINYINT,
    run_hour INT,
    departure_bus_times_cnt BIGINT,
    departure_bus_cnt BIGINT,
    running_mileage_sum DECIMAL(9,2),
    PRIMARY KEY (run_date, line_name, direction, run_hour) NOT ENFORCED
) WITH (
    'table-name' = 'rt_bus_operating_schedule_4_line',
    t{bdnode5_szb_visualization_mysql_conf}
);

-- 公交营运载客-线路维度(结果表)
CREATE TABLE rt_bus_operating_passengers_4_line (
    run_date DATE,
    line_name VARCHAR(20),
    direction TINYINT,
    run_hour INT,
    passenger_volume_cnt BIGINT,
    passenger_revenue_sum DECIMAL(9,2),
    actual_passenger_revenue_sum DECIMAL(9,2),
    PRIMARY KEY (run_date, line_name, direction, run_hour) NOT ENFORCED
) WITH (
    'table-name' = 'rt_bus_operating_passengers_4_line',
    t{bdnode5_szb_visualization_mysql_conf}
);


-- 公交营运异常发车间隔(结果表)
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
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table-name' = 'rt_abnormal_departure_interval',
    t{bdnode5_szb_visualization_mysql_conf}
);
