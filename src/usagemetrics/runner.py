import sys
import pandas as pd
from datetime import datetime, timedelta

from usagemetrics.metrics_writer import MetricsWriter
from usagemetrics.user_metrics_client import UserMetricsClient
from usagemetrics.prometheus_metrics_client import PrometheusClient

AUTO_PICK_CALENDAR_MONTH = "auto"


class UsageMetricsRunner:
    PROJECT_ID = "ClinEpiDB"

    def __init__(self, user_metrics_url, auth_key, prometheus_url, env, calendar_month):
        self.user_metrics_url = user_metrics_url
        self.auth_key = auth_key
        self.prometheus_url = prometheus_url
        self.env = env
        self.metrics_writer = MetricsWriter(None, None, None)
        self.prometheus_client = PrometheusClient(self.prometheus_url)
        if calendar_month == AUTO_PICK_CALENDAR_MONTH:
            current_date = datetime.today()
            self.start = first_day_of_month(current_date)
            self.end = last_day_of_month(current_date)
        else:
            calendar_month_tokens = calendar_month.split("-")
            self.start = datetime(year=int(calendar_month_tokens[0]), month=int(calendar_month_tokens[1]), day=1)
            self.end = last_day_of_month(self.start)

    def run(self):
        self.handle_analysis_metrics()
        self.handle_download_metrics()

    def handle_download_metrics(self):
        interval = (self.end - self.start).days
        download_metrics = self.prometheus_client.get_metrics(
            'increase(dataset_download_requested_total{environment="' + self.env + '"}[' + str(interval) + 'd])',
            start_date=self.start,
            end_date=self.end,
            labels=['user', 'study'])

        # Filter out zeroes before counting users per study
        download_metrics = download_metrics[download_metrics != 0.0]

        # group dataframe by study and count non-zero users
        download_metrics = download_metrics.groupby(axis=1, by=lambda x: x[1]).count()

        subsetting_download_metrics = self.prometheus_client.get_metrics(
            'increase(subset_download_requested_total{environment="' + self.env + '"}[' + str(interval) + 'd])',
            start_date=self.start,
            end_date=self.end,
            labels=['user_id', 'study_name'])

        subsetting_download_metrics = subsetting_download_metrics.groupby(axis=1, by=lambda x: x[1]).count()

        self.metrics_writer.write_download_histogram(download_metrics, "ds_downloads.csv", self.start.isoformat())
        self.metrics_writer.write_download_histogram(subsetting_download_metrics, "subsetting_downloads.csv", self.start.isoformat())

    def handle_analysis_metrics(self):
        user_metrics_client = UserMetricsClient(self.user_metrics_url, self.auth_key, self.PROJECT_ID)

        # (analysis_count_bucket, registered_user_count, guest_user_count, registered_user_filters, guest_user_filters)
        analysis_metrics = user_metrics_client.query_analysis_metrics(self.start, self.end)

        # quantize objects count into buckets to be stored as report output
        analysis_metrics['objects_bucket'] = pd.cut(analysis_metrics['objects_count'],
                                                    bins=[-1, 0, 1, 2, 4, 8, 16, 32, 64, sys.maxsize],
                                                    labels=["0", "1", "2", "<=4", "<=8", "<=16", "<=32", "<=64", ">64"])

        # group by new objects count bucket field to produce a histogram and write as output
        users_in_buckets = analysis_metrics.groupby("objects_bucket").sum()
        self.metrics_writer.write_analysis_histogram(users_in_buckets, self.start.isoformat())


def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - timedelta(days=next_month.day)


def first_day_of_month(any_day):
    return any_day.replace(day=1, hour=0, minute=0)

