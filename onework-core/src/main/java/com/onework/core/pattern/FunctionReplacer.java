package com.onework.core.pattern;

import com.onework.core.pattern.function.FunctionParser;
import com.onework.core.pattern.function.PlusDayFunction;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.onework.core.common.JobErrorMsg.FUNCTION_NOT_EXIST;

public class FunctionReplacer extends PatternReplacer {
    private Map<String, FunctionParser> functionParsers = new HashMap<>();

    public FunctionReplacer() {
        functionParsers.put("day", new PlusDayFunction());
    }

    @Override
    protected String tagPattern() {
        return "f|F";
    }

    @Override
    protected String afterReplace(String name, String[] argv) {
        FunctionParser functionParser = functionParsers.get(name);
        checkNotNull(functionParser, String.format(FUNCTION_NOT_EXIST, name));
        return functionParser.parse(argv);
    }
}
