@template{templateName=dispatch_card_kafka_conf};

'connector' = 'kafka',
'properties.bootstrap.servers' = '172.31.41.17:9092',
'scan.startup.mode' = 'group-offsets',
'format' = 'json',
'topic' = 'UD-PAY-CARD';