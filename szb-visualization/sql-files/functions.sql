@stream_sql{jobName=functions,dependent_job=true};

CREATE FUNCTION TO_DATE AS 'com.onework.function.flink.ToDate';
CREATE FUNCTION TO_DATE_STRING AS 'com.onework.function.flink.ToDateString';
CREATE FUNCTION TO_TIMESTAMP AS 'com.onework.function.flink.ToTimestamp';
CREATE FUNCTION IN_PERIOD AS 'com.onework.function.flink.InPeriod';
CREATE FUNCTION BUS_ABNORMAL_DEPARTURE AS 'com.onework.function.flink.BusAbnormalDeparture';
CREATE FUNCTION SZT_FILL_PAY AS 'com.onework.function.flink.SZTFillPay';
CREATE FUNCTION ON_THE_DAY AS 'com.onework.function.flink.OnTheDay';