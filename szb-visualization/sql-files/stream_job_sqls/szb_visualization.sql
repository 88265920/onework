@stream_sql{jobName=szb_visualization,core=4,minState=2,maxState=3,
    jars='/home/kangjiao/jars/onework-function-1.0.0-SNAPSHOT.jar',
    gp.stateRedisHost=10.128.3.54,gp.stateRedisDb=15};

@dependent_sql{jobName=functions};
@dependent_sql{jobName=szb_visualization_create};
@dependent_sql{jobName=szb_visualization_view};


------------------------------------------------------------------------------
-- 公交营运班次(结果维表数据写入)
INSERT INTO temporal_bus_operating_schedule
SELECT id,
    bus_id,
    bus_company,
    bus_team,
    peak_period,
    line_name,
    direction,
    run_date,
    TO_TIMESTAMP(actual_start_time) actual_start_time
FROM bus_operating_schedule_v2;


------------------------------------------------------------------------------
-- 公交营运班次统计输出(组织维度)
INSERT INTO rt_bus_operating_schedule_4_org
SELECT run_date,
    bus_company,
    IF(bus_team IS NULL, '', bus_team) bus_team,
    IF(peak_period IS NULL, '', peak_period) peak_period,
    COUNT(1) departure_bus_times_cnt,
    COUNT(DISTINCT bus_id) departure_bus_cnt,
    SUM(running_mileage) running_mileage_sum
FROM bus_operating_schedule_v2
WHERE ON_THE_DAY(actual_start_time)
GROUP BY GROUPING SETS((run_date, bus_company),
    (run_date, bus_company, bus_team),
    (run_date, bus_company, peak_period),
    (run_date, bus_company, bus_team, peak_period));

-- 公交营运载客统计输出(组织维度)
INSERT INTO rt_bus_operating_passengers_4_org
SELECT run_date,
    bus_company,
    IF(bus_team IS NULL, '', bus_team) bus_team,
    IF(peak_period IS NULL, '', peak_period) peak_period,
    COUNT(DISTINCT card_no) passenger_volume_cnt,
    SUM(txn_price) passenger_revenue_sum,
    SUM(txn_amount) actual_passenger_revenue_sum
FROM szt_pay_card_v2
WHERE ON_THE_DAY(txn_date)
GROUP BY GROUPING SETS((run_date, bus_company),
    (run_date, bus_company, bus_team),
    (run_date, bus_company, peak_period),
    (run_date, bus_company, bus_team, peak_period));


------------------------------------------------------------------------------
-- 公交营运班次统计输出(线路维度)
INSERT INTO rt_bus_operating_schedule_4_line
SELECT run_date,
    line_name,
    CAST(IF(direction IS NULL, -1, direction) AS TINYINT) direction,
    IF(run_hour IS NULL, -1, run_hour) run_hour,
    COUNT(1) departure_bus_times_cnt,
    COUNT(DISTINCT bus_id) departure_bus_cnt,
    SUM(running_mileage) running_mileage_sum
FROM bus_operating_schedule_v2
WHERE ON_THE_DAY(actual_start_time)
GROUP BY GROUPING SETS((run_date, line_name),
    (run_date, line_name, direction),
    (run_date, line_name, direction, run_hour),
    (run_date, bus_company, bus_team, peak_period));

-- 公交营运载客统计输出(线路维度)
INSERT INTO rt_bus_operating_passengers_4_line
SELECT run_date,
    line_name,
    CAST(IF(direction IS NULL, -1, direction) AS TINYINT) direction,
    IF(run_hour IS NULL, -1, run_hour) run_hour,
    COUNT(DISTINCT card_no) passenger_volume_cnt,
    SUM(txn_price) passenger_revenue_sum,
    SUM(txn_amount) actual_passenger_revenue_sum
FROM szt_pay_card_v2
WHERE ON_THE_DAY(txn_date)
GROUP BY GROUPING SETS((run_date, line_name),
    (run_date, line_name, direction),
    (run_date, line_name, direction, run_hour),
    (run_date, bus_company, bus_team, peak_period));


------------------------------------------------------------------------------
-- 公交营运异常发车间隔统计输出
INSERT INTO rt_abnormal_departure_interval
SELECT t2.id,
    t1.line_version_id,
    t1.bus_company,
    t1.bus_team,
    t1.line_name,
    t1.direction,
    t1.run_date,
    TO_DATE_STRING(t2.plan_start_time, 'HH:mm') plan_departure,
    TO_DATE_STRING(t2.actual_start_time, 'HH:mm') actual_departure,
    t2.check_interval,
    t2.abnormal_departure,
    t2.prev_log
FROM bus_operating_schedule_v2 t1,
    LATERAL TABLE(BUS_ABNORMAL_DEPARTURE(t1.id, t1.line_name, t1.direction, t1.plan_start_time, t1.actual_start_time, t1.check_interval))
        AS t2(id, plan_start_time, actual_start_time, check_interval, abnormal_departure, prev_log);