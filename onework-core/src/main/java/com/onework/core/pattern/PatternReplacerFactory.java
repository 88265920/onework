package com.onework.core.pattern;

/**
 * @author kangj
 * @date 2020/11/20
 **/

import com.onework.core.entity.SqlStatement;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class PatternReplacerFactory {
    private List<PatternReplacer> patternReplacers = new ArrayList<>();
    private List<PatternReplacer> runtimePatternReplacers = new ArrayList<>();

    public PatternReplacerFactory() {
        patternReplacers.add(new TemplateReplacer());
        runtimePatternReplacers.add(new FunctionReplacer());
    }

    public void patternReplace(List<SqlStatement> sqlStatements) {
        for (SqlStatement sqlStatement : sqlStatements) {
            sqlStatement.setSqlContent(patternReplace(sqlStatement.getSqlContent()));
        }
    }

    public void runtimePatternReplace(List<SqlStatement> sqlStatements) {
        for (SqlStatement sqlStatement : sqlStatements) {
            sqlStatement.setSqlContent(runtimePatternReplace(sqlStatement.getSqlContent()));
        }
    }

    private String patternReplace(String sqlContent) {
        String replaced = sqlContent;
        for (PatternReplacer replacer : patternReplacers) {
            replaced = replacer.replace(replaced);
        }
        return replaced;
    }

    private String runtimePatternReplace(String sqlContent) {
        String replaced = sqlContent;
        for (PatternReplacer replacer : runtimePatternReplacers) {
            replaced = replacer.replace(replaced);
        }
        return replaced;
    }
}
