package com.onework.function.flink;

import org.apache.flink.table.functions.ScalarFunction;

public class OnTheDay extends ScalarFunction {
    public boolean eval(Long ts) {
        int today = (int) System.currentTimeMillis() / 86400000;
        return ts / 86400000 >= today;
    }
}
