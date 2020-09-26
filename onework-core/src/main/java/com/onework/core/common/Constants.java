package com.onework.core.common;

public class Constants {

    public enum JobKind {
        STREAM_SQL, BATCH_SQL
    }

    public enum TemplateKind {
        TEMPLATE
    }

    public enum StatementKind {
        JOB_ENTRY, TEMPLATE_ENTRY, DEPENDENT_SQL, SQL_STATEMENT
    }

    public enum JobStatus {
        CREATED, RUNNING, SUSPEND, FAILED
    }

    public enum TemplateStatus {
        CREATED
    }

    public enum ResumeMethod {
        CHECKPOINT, SAVEPOINT
    }
}
