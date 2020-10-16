@batch_sql{jobName=delete_temporal_bus_operating_schedule,engine=jdbc,cronTime='0 0 0 * * ?',
    driver='com.mysql.jdbc.Driver',url='jdbc:mysql://bdnode5:3306/szb_visualization',user=root,password='1qaz@WSX'};

delete from temporal_bus_operating_schedule where run_date < f{day 'yyyy-MM-dd' -3};