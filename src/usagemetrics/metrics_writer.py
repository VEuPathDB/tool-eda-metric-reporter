import cx_Oracle

SCHEMA_NAME = "usagemetrics"


class MetricsWriter:

    def __init__(self, db, username, password):
        # self.connection = cx_Oracle.connect(
        #     username,
        #     password,
        #     db,
        #     encoding='utf-8',
        #     dry_run=True)
        print("Not yet implemented")

    def write_analysis_histogram(self, df, id):
        print(f"Analysis histogram for run ID: {id}.")
        print(df.to_string())
        print("Not yet implemented")

    def write_download_histogram(self, df, file_name, id):
        # sql = f'INSERT INTO {SCHEMA_NAME}.analysis_histogram VALUES(:1,:2,:3,:4,:5,:6)'
        # df_list = df.values.tolist()
        # n = 0
        # for i in df.iterrows():
        #     self.connection.execute(sql, df_list[n])
        #     n += 1

        print(f"Download histogram for run ID: {id} and file name: {file_name}.")
        print(df.to_string())
        print("Not yet implemented")

    def write_per_study_metrics(self, df, id):
        print(f"Per study metrics for run ID: {id}.")
        print(df.to_string)

    def write_raw_analysis(self, raw_metrics, id):
        print("Not yet implemented")

    def write_registered_totals_stats(self, stats, id):
        print(f"Total stats for run ID: {id}.")
        print("Registered stats :" + str(stats))

    def write_guest_totals_stats(self, stats, id):
        print(f"Total stats for run ID: {id}.")
        print("Registered stats :" + str(stats))
