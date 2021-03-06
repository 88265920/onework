package com.onework.core.pattern.function;

/**
 * @author kangj
 * @date 2020/11/20
 **/

import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkArgument;

public class PlusDayFunction implements FunctionParser {
    @Override
    public String parse(String[] argv) {
        checkArgument(argv != null && argv.length == 2);
        String pattern = argv[0].replace("'", "");
        int number = Integer.parseInt(argv[1]);
        return String.format("'%s'", plusDate(number).toString(pattern));
    }

    protected DateTime plusDate(int number) {
        return new DateTime().plusDays(number);
    }
}
