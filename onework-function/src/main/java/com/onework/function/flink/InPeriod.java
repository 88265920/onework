package com.onework.function.flink;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.shaded.org.joda.time.DateTime;

public class InPeriod extends ScalarFunction {
    public Boolean eval(Long timestamp, String startPeriod, String endPeriod) {
        int start = Integer.parseInt(startPeriod.replace(":", ""));
        int end = Integer.parseInt(endPeriod.replace(":", ""));
        int current = Integer.parseInt(new DateTime(timestamp).toString("HHmm"));
        return start <= current && current <= end;
    }
}
