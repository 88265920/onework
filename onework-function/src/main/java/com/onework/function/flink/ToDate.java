package com.onework.function.flink;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.shaded.org.joda.time.DateTime;

import java.sql.Date;

public class ToDate extends ScalarFunction {
    private DateTime dateTimeOf1970 = DateTime.parse("1970-01-01");

    public Date eval(Integer days) {
        return new Date(dateTimeOf1970.plusDays(days).getMillis());
    }
}
