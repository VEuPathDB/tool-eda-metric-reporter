-- These reports are expected to be produced monthly. This assumption is baked into the script and the data model.
CREATE TABLE usagemetrics.reports (
    report_id                 VARCHAR(36) NOT NULL,
    report_month              NUMBER NOT NULL,
    report_year               NUMBER NOT NULL,
    report_time               TIMESTAMP NOT NULL,
    raw_analysis_data         CLOB, -- Raw data used to render analysis tables
    PRIMARY KEY(report_id)
);

CREATE TABLE usagemetrics.analysishistogram (
    report_id                       VARCHAR(36) NOT NULL,
    count_bucket                    VARCHAR(20) NOT NULL, --Histogram bucket, e.g. 1, 2, <=4, <=8
    registered_users_analyses       NUMBER NOT NULL, --Counts of users in the object count bucket.
    guests_analyses                 NUMBER NOT NULL,
    registered_users_filters        NUMBER NOT NULL,
    guests_filters                  NUMBER NOT NULL,
    registered_users_visualizations NUMBER NOT NULL,
    guest_users_visualizations      NUMBER NOT NULL,
    PRIMARY KEY(report_id, count_bucket),
    FOREIGN KEY (report_id)
        REFERENCES usagemetrics.reports(report_id)
);

CREATE TABLE usagemetrics.analysismetricsperstudy (
    report_id                     VARCHAR(36) NOT NULL,
    dataset_id                    VARCHAR(20) NOT NULL, -- Note, this is a 1-1 mapping to study_id, but this is a different format
    analysis_count                NUMBER NOT NULL,
    shares_count                  NUMBER NOT NULL,
    PRIMARY KEY(report_id, dataset_id),
    FOREIGN KEY (report_id)
        REFERENCES usagemetrics.reports(report_id)
);

CREATE TABLE usagemetrics.aggregateuserstats (
    report_id                 VARCHAR(36) NOT NULL,
    user_category             VARCHAR(10) NOT NULL, -- "guest" OR "registered"
    num_users                 NUMBER NOT NULL,
    num_filters               NUMBER NOT NULL,
    num_analyses              NUMBER NOT NULL,
    num_visualizations        NUMBER NOT NULL,
    PRIMARY KEY(report_id, user_category),
    FOREIGN KEY (report_id)
        REFERENCES usagemetrics.reports(report_id)
);

CREATE TABLE usagemetrics.downloadsperstudy (
    report_id                     VARCHAR(36) NOT NULL,
    study_id                      VARCHAR(20) NOT NULL,
    num_users_full_download       NUMBER NOT NULL,
    num_users_subset_download     NUMBER NOT NULL,
    PRIMARY KEY(report_id, study_id),
    FOREIGN KEY (report_id)
        REFERENCES usagemetrics.reports(report_id)
);
