import json
import re

import cx_Oracle
import ldap

SCHEMA_NAME = "dmgaldi"


class MetricsWriter:

    def __init__(self, ldap_host, ldap_query, username, password):
        print(f"Connect with {ldap_host} {ldap_query}")
        ldap_connect = ldap.initialize(f'ldap://{ldap_host}')
        l_search = ldap_connect.search(base=ldap_query,
                                       scope=ldap.SCOPE_SUBTREE,
                                       filterstr="(&(cn=acctdbn)(objectClass=orclNetService))")
        result_status, result_data = ldap_connect.result(l_search, 0)
        print(result_status)
        desc = str(result_data[0][1]['orclNetDescString'][0])
        match = re.search("\\(HOST=([a-zA-Z0-9.-_]+)\\).*\\(PORT=([0-9]+)\\).*\\(SERVICE_NAME=([a-zA-Z0-9.-_]+)\\)", desc)
        host = match.group(1)
        port = match.group(2)
        service = match.group(3)
        # TODO: Enable this connection once tables are created and credentials are available in env.
        self.connection = cx_Oracle.connect(
            username,
            password,
            f"{host}:{port}/{service}",
            encoding='utf-8')

    def create_job(self, id, start_month):
        sql = '''INSERT INTO {0}.job_runs (run_id, start_month, active) 
                    VALUES(:1,:2,:3)'''.format(SCHEMA_NAME)
        cursor = self.connection.cursor()
        cursor.execute(sql, [id, start_month, 1])
        self.connection.commit()

    def write_analysis_histogram(self, df, run_id):
        sql = '''
        INSERT INTO {0}.analysis_histogram (run_id, count_bucket, registered_users_analyses, guests_analyses, registered_users_filters, guests_filters) 
            VALUES(:1,:2,:3,:4,:5,:6)
        '''.format(SCHEMA_NAME)
        print(df.to_string())
        df = df.fillna(0)
        for (bucket, data) in df.iterrows():
            print([run_id, bucket,
                   data['registered_users_with_analysis_count'],
                   data['guest_users_with_analysis_count'],
                   data['registered_users_with_filter_count'],
                   data['guest_users_with_filter_count']])
            cursor = self.connection.cursor()
            cursor.execute(sql, [run_id, bucket,
                                 data['registered_users_with_analysis_count'],
                                 data['guest_users_with_analysis_count'],
                                 data['registered_users_with_filter_count'],
                                 data['guest_users_with_filter_count']])
            self.connection.commit()

    def write_downloads_by_study(self, df, run_id):
        sql = '''
        INSERT INTO {0}.downloads_per_study 
            (run_id, study_id, num_users_full_download, num_users_subset_download) 
            VALUES(:1,:2,:3,:4)
        '''.format(SCHEMA_NAME)
        df = df.fillna(0)
        cursor = self.connection.cursor()
        for (study_name, data) in df.iterrows():
            cursor.execute(sql, [run_id, study_name] + list(data.values))
            print([run_id, study_name, data["file_downloads"], data["subset_downloads"]])
        self.connection.commit()

    def write_analysis_metrics_by_study(self, df, run_id):
        sql = '''
        INSERT INTO {0}.analysis_metrics_per_study 
            (run_id, dataset_id, analysis_count, shares_count) 
            VALUES(:1,:2,:3,:4)
        '''.format(SCHEMA_NAME)
        df = df.fillna(0)
        cursor = self.connection.cursor()
        for (_, data) in df.iterrows():
            cursor.execute(sql, [run_id, data['study_id'], data['analysis_count'], data['shares_count']])
            print([run_id, data['study_id'], data['analysis_count'], data['shares_count']])
        self.connection.commit()

    def write_raw_analysis(self, raw_metrics, run_id):
        sql = '''
            UPDATE {0}.job_runs
              SET raw_analysis_data = :1
              WHERE run_id = :2
        '''.format(SCHEMA_NAME)
        cursor = self.connection.cursor()
        cursor.execute(sql, [json.dumps(raw_metrics), run_id])
        self.connection.commit()

    def write_aggregate_stats(self, stats, run_id, user_type):
        sql = f'INSERT INTO {SCHEMA_NAME}.aggregate_user_stats (run_id, user_category, num_users, num_analyses, num_filters, num_visualizations) VALUES(:1,:2,:3,:4,:5,:6)'
        cursor = self.connection.cursor()
        cursor.execute(sql, [run_id, user_type, stats['numUsers'], stats['numAnalyses'], stats['numFilters'], stats['numVisualizations']])
        self.connection.commit()
        print([run_id, user_type, stats['numUsers'], stats['numAnalyses'], stats['numFilters'], stats['numVisualizations']])
