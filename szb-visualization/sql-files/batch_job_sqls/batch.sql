@batch_sql(engine=jdbc,cronTime='0 0 * * * ?',driver='com.mysql.jdbc.Driver',
    url='jdbc:mysql://bdnode5:3306/szb_visualization',user=root,password='1qaz@WSX');

delete from temporal_bus_operating_schedule where run_date < '2020-09-12';