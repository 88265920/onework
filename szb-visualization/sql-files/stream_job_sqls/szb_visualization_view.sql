@stream_sql{jobName=szb_visualization_view,dependent_job=true};

-- 公交营运班次1层视图(关联组织信息)
CREATE VIEW bus_operating_schedule_v1 AS
SELECT t1.id id,
    TO_DATE(t1.run_date) run_date,
    t1.line_version_id,
    t1.direction,
    t1.bus_id,
    t1.plan_distance running_mileage,
    t1.plan_time_start - 28800000 plan_start_time,
    t1.real_time_start - 28800000 actual_start_time,
    t2.line_name,
    t3.org_name bus_team,
    t4.org_name bus_company,
    t5.route_id,
    t1.proc_time
FROM tbl_ehualu_trip t1
LEFT JOIN tbl_base_line FOR SYSTEM_TIME AS OF t1.proc_time t2
    ON t1.line_version_id = t2.line_version_id
LEFT JOIN tbl_base_organization FOR SYSTEM_TIME AS OF t1.proc_time t3
    ON t2.org_id = t3.org_id
LEFT JOIN tbl_base_organization FOR SYSTEM_TIME AS OF t1.proc_time t4
    ON t3.org_parent_id = t4.org_id
LEFT JOIN tbl_base_line_route FOR SYSTEM_TIME AS OF t1.proc_time t5
    ON t1.line_version_id = t5.line_version_id AND t1.direction = t5.route_direction
        AND t5.route_operate_type = CAST(1 AS TINYINT)
WHERE t1.trip_type = 1 AND t1.record_status in (1, 2, 21) AND t1.real_time_start IS NOT NULL;


-- 公交营运班次2层视图(关联峰段信息)
CREATE VIEW bus_operating_schedule_v2 AS
SELECT t1.*,
    CAST(HOUR(TO_TIMESTAMP(t1.actual_start_time)) AS INT) run_hour,
    t3.period_name,
    CASE t3.period_peak WHEN 1 THEN '高峰期' ELSE '平峰期' END peak_period,
    t3.check_interval
FROM bus_operating_schedule_v1 t1
LEFT JOIN tbl_base_calendar FOR SYSTEM_TIME AS OF t1.proc_time t2
    ON t1.run_date = t2.calendar_date
LEFT JOIN tbl_base_route_period FOR SYSTEM_TIME AS OF t1.proc_time t3
    ON t1.route_id = t3.route_id AND t2.calendar_type = t3.period_type
        AND IN_PERIOD(t1.actual_start_time, t3.period_start_time, t3.period_end_time)
        AND t3.check_interval IS NOT NULL
WHERE t3.period_name IS NOT NULL;


------------------------------------------------------------------------------
-- 深圳通刷卡数据1层转换视图(计算退票的实际车牌号)
CREATE VIEW szt_pay_card_v1 AS
SELECT *, SZT_FILL_PAY(txnType, txnDate, cardNo, busNo) actual_bus_no
FROM szt_pay_card;

-- 深圳通刷卡数据2层转换视图(关联基础维表数据和排班调度结果维表数据，计算刷卡数据对应到排班的实际车辆的线路)
CREATE VIEW szt_pay_card_v2 AS
SELECT CAST(t1.ts AS DATE) run_date,
    CAST(HOUR(t1.ts) AS INT) run_hour,
    t3.bus_company,
    t3.bus_team,
    t3.peak_period,
    t3.line_name,
    t3.direction,
    t1.txnDate txn_date,
    t1.cardNo card_no,
    t1.txnPrice / 100 txn_price,
    t1.txnAmount / 100 txn_amount,
    t1.proc_time
FROM szt_pay_card_v1 t1
LEFT JOIN tbl_base_bus FOR SYSTEM_TIME AS OF t1.proc_time t2
    ON CONCAT('粤B', t1.actual_bus_no) = t2.bus_plate_number
LEFT JOIN temporal_bus_operating_schedule FOR SYSTEM_TIME AS OF t1.proc_time t3
    ON t2.bus_id = t3.bus_id AND CAST(t1.ts AS DATE) = t3.run_date AND t1.ts > t3.actual_start_time
        AND t1.ts < t3.actual_start_time + INTERVAL '3' HOUR
WHERE t2.bus_id IS NOT NULL;
