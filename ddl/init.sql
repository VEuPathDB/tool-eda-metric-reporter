CREATE TABLE usagemetrics.analysis_histogram (
    run_id                    VARCHAR(20) NOT NULL,
    count_bucket              VARCHAR(20) NOT NULL,
    registered_users_analyses NUMBER,
    guests_analyses           NUMBER,
    registered_users_filters  NUMBER,
    guests_filters            NUMBER,
    PRIMARY KEY(id, count_bucket)
);

CREATE TABLE usagemetrics.raw_analysis_data (
    run_id                  VARCHAR(20) NOT NULL,
    raw_data                CLOB,
    PRIMARY KEY(id)
);

CREATE TABLE usagemetrics.downloads_histogram (
    run_id                        VARCHAR(20) NOT NULL,
    study_id                      VARCHAR(20) NOT NULL,
    num_users_full_download       NUMBER,
    num_users_subset_download     NUMBER,
    PRIMARY KEY(id, study_id)
);

CREATE TABLE usagemetrics.aggregate_user_stats (
    run_id                    VARCHAR(20) NOT NULL,
    count                     NUMBER,
    num_filters               NUMBER,
    num_visualizations        NUMBER,
    user_category             ENUM('guest', 'registered'),
    PRIMARY KEY(id, study_id)
);
