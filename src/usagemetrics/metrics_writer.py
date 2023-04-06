import datetime
import json
import re

import cx_Oracle
import subprocess

SCHEMA_NAME = "usagemetrics"


class MetricsWriter:
    ORACLE_DESC_FIELD_NAME = 'orclNetDescString'

    def __init__(self, ldap_host, ldap_query, username, password, acctdb):
        print(f"Connect with {ldap_host} {ldap_query}")
        # Query LDAP for db connect information.
        args = ['ldapsearch', '-x', '-H', f"ldaps://{ldap_host}", '-b', ldap_query, '-s', 'sub', f"(&(cn={acctdb})(objectClass=orclNetService))", 'orclNetDescString']
        print("Running command with args: " + str(args))
        output = subprocess.check_output(args).decode('utf-8').replace("\n", '').replace(" ", "")
        match = re.search("HOST=([A-Za-z0-9.]+).*PORT=([0-9]+).*SERVICE_NAME=([a-zA-Z0-9.]+)", output)
        host = match.group(1)
        port = match.group(2)
        service = match.group(3)

        # Construct db connect information with parsed details from LDAP.
        self.connection = cx_Oracle.connect(
            username,
            password,
            f"{host}:{port}/{service}",
            encoding='utf-8')

    def create_job(self, report_id, start_month, start_year):
        sql = '''INSERT INTO {0}.reports (report_id, report_month, report_year, report_time) 
                    VALUES(:1,:2,:3,:4)'''.format(SCHEMA_NAME)
        cursor = self.connection.cursor()
        cursor.execute(sql, [report_id, start_month, start_year, datetime.datetime.now()])
        self.connection.commit()

    def write_analysis_histogram(self, df, report_id):
        sql = '''
        INSERT INTO {0}.analysishistogram 
            (report_id, count_bucket, registered_users_analyses, guests_analyses, registered_users_filters, guests_filters, registered_users_visualizations, guest_users_visualizations) 
            VALUES(:1,:2,:3,:4,:5,:6,:7,:8)
        '''.format(SCHEMA_NAME)
        print(df.to_string())
        df = df.fillna(0)
        for (bucket, data) in df.iterrows():
            print([report_id, bucket,
                   data['registered_users_with_analysis_count'],
                   data['guest_users_with_analysis_count'],
                   data['registered_users_with_filter_count'],
                   data['guest_users_with_filter_count'],
                   data['registered_users_with_viz_count'],
                   data['guest_users_with_viz_count']])
            cursor = self.connection.cursor()
            cursor.execute(sql, [report_id, bucket,
                                 data['registered_users_with_analysis_count'],
                                 data['guest_users_with_analysis_count'],
                                 data['registered_users_with_filter_count'],
                                 data['guest_users_with_filter_count'],
                                 data['registered_users_with_viz_count'],
                                 data['guest_users_with_viz_count']])
            self.connection.commit()

    def write_downloads_by_study(self, df, report_id):
        sql = '''
        INSERT INTO {0}.downloadsperstudy 
            (report_id, study_id, num_users_full_download, num_users_subset_download) 
            VALUES(:1,:2,:3,:4)
        '''.format(SCHEMA_NAME)
        df = df.fillna(0)
        cursor = self.connection.cursor()
        for (study_name, data) in df.iterrows():
            cursor.execute(sql, [report_id, study_name] + list(data.values))
            print([report_id, study_name, data["file_downloads"], data["subset_downloads"]])
        self.connection.commit()

    def write_analysis_metrics_by_study(self, df, report_id):
        sql = '''
        INSERT INTO {0}.analysismetricsperstudy 
            (report_id, dataset_id, analysis_count, shares_count) 
            VALUES(:1,:2,:3,:4)
        '''.format(SCHEMA_NAME)
        df = df.fillna(0)
        cursor = self.connection.cursor()
        for (_, data) in df.iterrows():
            cursor.execute(sql, [report_id, data['study_id'], data['analysis_count'], data['shares_count']])
            print([report_id, data['study_id'], data['analysis_count'], data['shares_count']])
        self.connection.commit()

    def write_raw_analysis(self, raw_metrics, report_id):
        sql = '''
            UPDATE {0}.reports
              SET raw_analysis_data = :1
              WHERE report_id = :2
        '''.format(SCHEMA_NAME)
        cursor = self.connection.cursor()
        cursor.execute(sql, [json.dumps(raw_metrics), report_id])
        self.connection.commit()

    def write_aggregate_stats(self, stats, report_id, user_type):
        sql = f'INSERT INTO {SCHEMA_NAME}.aggregateuserstats (report_id, user_category, num_users, num_analyses, num_filters, num_visualizations) VALUES(:1,:2,:3,:4,:5,:6)'
        cursor = self.connection.cursor()
        cursor.execute(sql, [report_id, user_type, stats['numUsers'], stats['numAnalyses'], stats['numFilters'], stats['numVisualizations']])
        self.connection.commit()
        print([report_id, user_type, stats['numUsers'], stats['numAnalyses'], stats['numFilters'], stats['numVisualizations']])
