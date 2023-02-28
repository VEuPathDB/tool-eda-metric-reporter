import cx_Oracle

SCHEMA_NAME = "usagemetrics"


class MetricsWriter:

    def __init__(self, db, username, password):
        print("NYI")
        # TODO: Enable this connection once tables are created and credentials are available in env.
        # self.connection = cx_Oracle.connect(
        #     username,
        #     password,
        #     db,
        #     encoding='utf-8',
        #     dry_run=True)

    def create_job(self, id, start_month):
        sql = '''INSERT INTO {0}.job_runs (run_id, start_month, active) 
                    VALUES(:1,:2,:3)'''.format(SCHEMA_NAME)
        # self.connection.execute(sql, [id, start_month, 1])

    def write_analysis_histogram(self, df, id):
        sql = '''
        INSERT INTO {0}.analysis_histogram 
            (run_id, count_bucket, registered_users_analyses, guests_analyses, registered_users_filters, guests_filters) 
            VALUES(:1,:2,:3,:4:,:5,:6)
        '''.format(SCHEMA_NAME)
        print(df.to_string())
        for (bucket, data) in df.iterrows():
            # self.connection.execute(sql, [id, bucket,
            #                    data['registered_users_with_analysis_count'],
            #                    data['guest_users_with_analysis_count'],
            #                    data['registered_users_with_filter_count'],
            #                    data['guest_users_with_filter_count']])
            print([id, bucket,
                   data['registered_users_with_analysis_count'],
                   data['guest_users_with_analysis_count'],
                   data['registered_users_with_filter_count'],
                   data['guest_users_with_filter_count']])

    def write_downloads_by_study(self, df, id):
        sql = '''
        INSERT INTO {0}.download_histogram 
            (run_id, study_id, num_users_full_download, num_users_partial_download) 
            VALUES(:1,:2,:3,:4)
        '''.format(SCHEMA_NAME)
        for (study_name, data) in df.iterrows():
            # self.connection.execute(sql, [id, study_name] + list(data.values))
            print([id, study_name, data["file_downloads"], data["subset_downloads"]])

    def write_analysis_metrics_by_study(self, df, id):
        sql = '''
        INSERT INTO {0}.download_histogram 
            (run_id, study_id, num_users_full_download, num_users_partial_download) 
            VALUES(:1,:2,:3,:4)
        '''.format(SCHEMA_NAME)
        for (_, data) in df.iterrows():
            # self.connection.execute(sql, [id, data['study_id'], data['analysis_count'], data['shares_count']])
            print([id, data['study_id'], data['analysis_count'], data['shares_count']])

    def write_raw_analysis(self, raw_metrics, id):
        sql = '''
            UPDATE {0}.job_runs
              SET raw_analysis_data = :1
              WHERE run_id = :2
        '''.format(SCHEMA_NAME)
        # self.connection.execute(sql, [raw_metrics, id])

    def write_aggregate_stats(self, stats, id, user_type):
        sql = f'INSERT INTO {SCHEMA_NAME}.aggregate_user_stats (run_id, user_category, num_users, num_filters, num_visualizations) VALUES(:1,:2,:3,:4,:5)'
        # self.connection.execute(sql, [id, user_type, stats['numUsers'], stats['numAnalyses'], stats['numFilters'], stats['numVisualizations']])
        print([id, user_type, stats['numUsers'], stats['numAnalyses'], stats['numFilters'], stats['numVisualizations']])
