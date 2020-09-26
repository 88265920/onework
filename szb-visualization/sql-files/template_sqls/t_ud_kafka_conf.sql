@template{templateName=ud_kafka_conf};

'connector' = 'kafka',
'properties.bootstrap.servers' = '10.128.3.52:9092,10.128.3.53:9092,10.128.3.54:9092',
'scan.startup.mode' = 'group-offsets';