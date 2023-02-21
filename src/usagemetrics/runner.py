import sys
import pandas as pd
from datetime import datetime, timedelta

from usagemetrics.metrics_writer import MetricsWriter
from usagemetrics.user_metrics_client import UserMetricsClient
from usagemetrics.prometheus_metrics_client import PrometheusClient


class UsageMetricsRunner:
    PROJECT_ID = "ClinEpiDB"

    def __init__(self, user_metrics_url, auth_key, prometheus_url, env):
        self.user_metrics_url = user_metrics_url
        self.auth_key = auth_key
        self.prometheus_url = prometheus_url
        self.env = env
        self.metrics_writer = MetricsWriter(None, None, None)
        self.prometheus_client = PrometheusClient(self.prometheus_url)

    def run(self):
        self.handle_analysis_metrics()
        self.handle_download_metrics()

    def handle_download_metrics(self):
        current_date = datetime.today()
        prom_query_start = first_day_of_month(current_date)
        prom_query_end = last_day_of_month(current_date)

        download_metrics = self.prometheus_client.get_metrics(
            "increase(dataset_download_requested_total{environment=" + self.env + "}[1d])",
            start_date=prom_query_start,
            end_date=prom_query_end,
            labels=['user', 'study'])

        # Filter out zeroes before counting users per study
        download_metrics = download_metrics[download_metrics != 0.0]

        # group dataframe by study and count non-zero users
        download_metrics = download_metrics.groupby(axis=1, by=lambda x: x[1]).count()

        subsetting_download_metrics = self.prometheus_client.get_metrics(
            'increase(subset_download_requested_total{environment=' + self.env + '}[1d])',
            start_date=prom_query_start,
            end_date=prom_query_end,
            labels=['user_id', 'study_name'])

        subsetting_download_metrics = subsetting_download_metrics.groupby(axis=1, by=lambda x: x[1]).count()

        self.metrics_writer.write_download_histogram(download_metrics, "ds_downloads.csv")
        self.metrics_writer.write_download_histogram(subsetting_download_metrics, "subsetting_downloads.csv")

    def handle_analysis_metrics(self):
        user_metrics_client = UserMetricsClient(self.user_metrics_url, self.auth_key, self.PROJECT_ID)

        # (analysis_count_bucket, registered_user_count, guest_user_count, registered_user_filters, guest_user_filters)
        analysis_metrics = user_metrics_client.query_analysis_metrics()

        # quantize objects count into buckets to be stored as report output
        analysis_metrics['objects_bucket'] = pd.cut(analysis_metrics['objects_count'],
                                                    bins=[-1, 0, 1, 2, 4, 8, 16, 32, 64, sys.maxsize],
                                                    labels=["0", "1", "2", "<=4", "<=8", "<=16", "<=32", "<=64", ">64"])

        # group by new objects count bucket field to produce a histogram and write as output
        users_in_buckets = analysis_metrics.groupby("objects_bucket").sum()
        self.metrics_writer.write_analysis_histogram(users_in_buckets)


def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - timedelta(days=next_month.day)


def first_day_of_month(any_day):
    return any_day.replace(day=1)

