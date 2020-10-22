package com.onework.core.pattern;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.onework.core.common.JobErrorMsg.PATTERN_NAME_ERROR;
import static com.onework.core.common.JobErrorMsg.PATTERN_PARAM_ERROR;

public abstract class PatternReplacer {
    private static final String TAG_PATTERN_FORMAT = "(%s)\\{[\\'a-zA-Z0-9_-]+((,[\\'a-zA-Z0-9_-]+)+)?\\}$";

    protected abstract String tagPattern();

    protected String regularPattern() {
        return String.format(TAG_PATTERN_FORMAT, tagPattern());
    }

    protected abstract String afterReplace(String name, String[] argv);

    public String replace(String sqlStatement) {
        StringBuffer sb = new StringBuffer();
        Pattern pattern = Pattern.compile(regularPattern());
        Matcher matcher = pattern.matcher(sqlStatement);
        while (matcher.find()) {
            String matched = matcher.group();

            String[] argv = matched.substring(2, matched.length() - 1).split(",");
            String name = argv[0];
            checkArgument(StringUtils.isNotEmpty(name), PATTERN_NAME_ERROR);

            String afterReplace;
            if (argv.length <= 1) {
                afterReplace = afterReplace(name, null);
            } else {
                List<String> argvList = Lists.newArrayList(argv);
                argvList.remove(0);
                checkArgument(CollectionUtils.isNotEmpty(argvList), PATTERN_PARAM_ERROR);
                afterReplace = afterReplace(name, argvList.toArray(new String[0]));
            }

            matcher.appendReplacement(sb, afterReplace);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
