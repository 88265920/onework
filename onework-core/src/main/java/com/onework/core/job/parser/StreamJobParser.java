package com.onework.core.job.parser;

import com.onework.core.entity.JobEntry;
import com.onework.core.entity.SqlStatement;
import com.onework.core.entity.StreamJob;
import com.onework.core.enums.JobKind;
import com.onework.core.enums.StatementKind;
import com.onework.core.job.parser.statement.DependentSqlParser;
import com.onework.core.job.parser.statement.JobEntryParser;
import com.onework.core.job.parser.statement.SqlStatementParser;
import com.onework.core.job.parser.statement.StatementParser;
import com.onework.core.service.TemplateService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@Component
public class StreamJobParser extends BaseJobParser<StreamJob> {

    private TemplateService templateService;

    public StreamJobParser(TemplateService templateService) {
        this.templateService = templateService;
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
        List<SqlStatement> jobSqlStatements = new ArrayList<>();
        for (Map<String, Object> statementData : statementsData) {
            StatementKind statementKind = getStatementKind(statementData);
            switch (statementKind) {
                case JOB_ENTRY:
                    Map<String, String> jobParams = (Map<String, String>) statementData.get("jobParams");
                    String jobName = jobParams.get("jobName");
                    checkState(StringUtils.isNotEmpty(jobName));
                    JobKind jobKind = (JobKind) statementData.get("jobKind");
                    checkNotNull(jobKind);
                    JobEntry jobEntry = new JobEntry(jobName, jobKind, jobParams);
                    streamJob.setJobName(jobName);
                    streamJob.setJobEntry(jobEntry);
                    break;
                case DEPENDENT_SQL:
                    Map<String, String> dependentParams = (Map<String, String>) statementData.get("dependentParams");
                    checkNotNull(dependentParams);
                    jobName = dependentParams.get("jobName");
                    checkState(StringUtils.isNotEmpty(jobName));
                    dependentJobNames.add(jobName);
                    break;
                case SQL_STATEMENT:
                    checkState(statementData.containsKey("sqlStatements"));
                    List<SqlStatement> sqlStatements = ((List<String>) statementData.get("sqlStatements")).stream()
                            .map(s -> new SqlStatement(streamJob.getJobName(), s)).collect(Collectors.toList());
                    templateService.templateReplace(sqlStatements);
                    jobSqlStatements.addAll(sqlStatements);
                    break;
                default:
                    break;
            }
        }
        streamJob.setDependentJobNames(dependentJobNames);
        streamJob.setSqlStatements(jobSqlStatements);
        return streamJob;
    }
}
