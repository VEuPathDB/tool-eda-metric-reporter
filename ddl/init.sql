CREATE TABLE usagemetrics.job_runs (
    run_id                    VARCHAR(20) NOT NULL,
    start_month               DATE NOT NULL,
    active                    NUMBER(1,0) NOT NULL,
    raw_analysis_data         CLOB
)

CREATE TABLE usagemetrics.analysis_histogram (
    run_id                    VARCHAR(20) NOT NULL,
    count_bucket              VARCHAR(20) NOT NULL,
    registered_users_analyses NUMBER NOT NULL,
    guests_analyses           NUMBER NOT NULL,
    registered_users_filters  NUMBER NOT NULL,
    guests_filters            NUMBER NOT NULL,
    PRIMARY KEY(run_id, count_bucket),
    FOREIGN KEY (run_id)
        REFERENCES usagemetrics.job_runs(run_id)
);

CREATE TABLE usagemetrics.downloads_per_study (
    run_id                        VARCHAR(20) NOT NULL,
    study_id                      VARCHAR(20) NOT NULL,
    num_users_full_download       NUMBER NOT NULL,
    num_users_subset_download     NUMBER NOT NULL,
    PRIMARY KEY(id, study_id)
    FOREIGN KEY (run_id)
        REFERENCES usagemetrics.job_runs(run_id)
);

CREATE TABLE usagemetrics.analysis_metrics_per_study (
    run_id                        VARCHAR(20) NOT NULL,
    dataset_id                    VARCHAR(20) NOT NULL, -- Note, this is a 1-1 mapping to study_id, but this is a different format
    analysis_count                NUMBER NOT NULL,
    shares_count                  NUMBER NOT NULL,
    PRIMARY KEY(run_id, dataset_id)
    FOREIGN KEY (run_id)
        REFERENCES usagemetrics.job_runs(run_id)
);


CREATE TABLE usagemetrics.aggregate_user_stats (
    run_id                    VARCHAR(20) NOT NULL,
    user_category             ENUM('guest', 'registered') NOT NULL,
    num_users                 NUMBER NOT NULL,
    num_filters               NUMBER NOT NULL,
    num_visualizations        NUMBER NOT NULL,
    PRIMARY KEY(run_id, user_category)
    FOREIGN KEY (run_id)
        REFERENCES usagemetrics.job_runs(run_id)
);
