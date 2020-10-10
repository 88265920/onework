@template{templateName=dispatch_slave_base_data_mysql_conf};

'connector' = 'jdbc',
'url' = 'jdbc:mysql://172.31.11.22:3306/db_base_data?characterEncoding=UTF-8&tinyInt1isBit=false&useSSL=false',
'username' = 'reader',
'password' = 'reader',
'lookup.cache.max-rows' = '10000',
'lookup.cache.ttl' = '300s';