package com.onework.core.job.executor.impl;

import com.onework.core.entity.BatchJob;
import com.onework.core.entity.SqlStatement;
import com.onework.core.job.executor.BatchJobExecutor;
import com.onework.core.job.executor.ExecutePositionTracker;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class JDBCBatchJobExecutor implements BatchJobExecutor {

    @SneakyThrows
    @Override
    public void executeJob(String fireTime, @NonNull BatchJob job, @Nonnull ExecutePositionTracker tracker) {
        Map<String, String> jobParams = job.getJobEntry().getJobParams();
        checkArgument(MapUtils.isNotEmpty(jobParams));
        String driver = jobParams.get("driver");
        checkArgument(StringUtils.isNotEmpty(driver));
        String url = jobParams.get("url");
        checkArgument(StringUtils.isNotEmpty(url));
        String user = jobParams.get("user");
        String password = jobParams.get("password");

        Class.forName(driver);
        long costMillis = 0;
        Connection connection = null;
        Statement statement = null;
        try {
            connection = getConnection(url, user, password);
            statement = connection.createStatement();
            List<SqlStatement> sqlStatements = job.getSqlStatements();
            for (int i = 0; i < sqlStatements.size(); i++) {
                SqlStatement sqlStatement = sqlStatements.get(i);
                if (costMillis > 900000) {
                    statement.close();
                    connection.close();
                    connection = getConnection(url, user, password);
                    statement = connection.createStatement();
                    costMillis = 0;
                }
                costMillis += executeSql(sqlStatement.getSqlContent(), statement);
                tracker.executePosition(job.getJobName(), fireTime, i);
            }
        } finally {
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        }
    }

    @SneakyThrows
    private Connection getConnection(String url, String user, String password) {
        Connection connection;
        if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
            connection = DriverManager.getConnection(url, user, password);
        } else {
            connection = DriverManager.getConnection(url);
        }

        return connection;
    }

    @SneakyThrows
    private long executeSql(String sql, Statement statement) {
        long startMillis = System.currentTimeMillis();
        String selectString = sql.trim().substring(0, 6).toUpperCase();
        log.info("selectString={}", selectString);
        if (!selectString.equals("SELECT")) {
            statement.execute(sql);
        }
        return System.currentTimeMillis() - startMillis;
    }
}
