package com.onework.function.flink;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.shaded.org.joda.time.DateTime;

public class ToDateString extends ScalarFunction {
    public String eval(Long timestamp, String pattern) {
        return new DateTime(timestamp).toString(pattern);
    }
}
