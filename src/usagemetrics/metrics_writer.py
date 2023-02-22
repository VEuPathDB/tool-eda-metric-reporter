

class MetricsWriter:

    def __init__(self, db, username, password):
        print("Not yet implemented")

    def write_analysis_histogram(self, df, id):
        print(f"Analysis histogram for run ID: {id}.")
        print(df.to_string())
        print("Not yet implemented")

    def write_download_histogram(self, df, file_name, id):
        print(f"Download histogram for run ID: {id} and file name: {file_name}.")
        print(df.to_string())
        print("Not yet implemented")

    def write_raw_analysis(self):
        print("Not yet implemented")
