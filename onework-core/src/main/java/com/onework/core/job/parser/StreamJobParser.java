package com.onework.core.job.parser;

import com.onework.core.entity.StreamJob;
import com.onework.core.entity.StreamSqlStatement;
import com.onework.core.enums.JobKind;
import com.onework.core.enums.StatementKind;
import com.onework.core.job.parser.statement.DependentSqlParser;
import com.onework.core.job.parser.statement.JobEntryParser;
import com.onework.core.job.parser.statement.SqlStatementParser;
import com.onework.core.job.parser.statement.StatementParser;
import com.onework.core.pattern.PatternReplacerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

@Component
public class StreamJobParser extends BaseJobParser<StreamJob> {
    private PatternReplacerFactory patternReplacerFactory;

    @Autowired
    public StreamJobParser(PatternReplacerFactory patternReplacerFactory) {
        this.patternReplacerFactory = patternReplacerFactory;
    }

    @Override
    protected void bindParser(Map<StatementKind, StatementParser> statementParsers) {
        statementParsers.put(StatementKind.JOB_ENTRY, new JobEntryParser());
        statementParsers.put(StatementKind.DEPENDENT_SQL, new DependentSqlParser());
        statementParsers.put(StatementKind.SQL_STATEMENT, new SqlStatementParser());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected StreamJob onCreateJob(List<Map<String, Object>> statementsData) {
        StreamJob streamJob = new StreamJob();
        List<String> dependentJobNames = new ArrayList<>();
        List<StreamSqlStatement> jobStreamSqlStatements = new ArrayList<>();
        for (Map<String, Object> statementData : statementsData) {
            StatementKind statementKind = getStatementKind(statementData);
            switch (statementKind) {
                case JOB_ENTRY:
                    parseJobEntry(statementData, streamJob);
                    break;
                case DEPENDENT_SQL:
                    parseDependentJob(statementData, dependentJobNames);
                    break;
                case SQL_STATEMENT:
                    checkArgument(statementData.containsKey("sqlStatements"));
                    List<StreamSqlStatement> streamSqlStatements = ((List<String>) statementData.get("sqlStatements")).stream()
                            .map(s -> new StreamSqlStatement(streamJob.getJobName(), JobKind.STREAM_SQL, s))
                            .collect(Collectors.toList());
                    patternReplacerFactory.patternReplace(streamSqlStatements);
                    jobStreamSqlStatements.addAll(streamSqlStatements);
                    break;
                default:
                    break;
            }
        }
        streamJob.setDependentJobNames(dependentJobNames);
        streamJob.setStreamSqlStatements(jobStreamSqlStatements);
        return streamJob;
    }
}
