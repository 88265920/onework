package com.onework.function.flink;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class ToTimestamp extends ScalarFunction {
    public Timestamp eval(Long timestamp) {
        return new Timestamp(timestamp);
    }
}
