package com.onework.core.pattern;

import com.onework.core.entity.StreamSqlStatement;
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

    public void patternReplace(List<StreamSqlStatement> streamSqlStatements) {
        for (StreamSqlStatement streamSqlStatement : streamSqlStatements) {
            streamSqlStatement.setSqlContent(patternReplace(streamSqlStatement.getSqlContent()));
        }
    }

    public void runtimePatternReplace(List<StreamSqlStatement> streamSqlStatements) {
        for (StreamSqlStatement streamSqlStatement : streamSqlStatements) {
            streamSqlStatement.setSqlContent(runtimePatternReplace(streamSqlStatement.getSqlContent()));
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
