import sys
import uuid

import pandas as pd
from datetime import datetime, timedelta

from usagemetrics.analysis_metrics import AnalysisMetrics
from usagemetrics.eda_user_metrics_client import EdaUserServiceMetricsClient
from usagemetrics.prometheus_metrics_client import PrometheusClient

AUTO_PICK_CALENDAR_MONTH = "auto"


class UsageMetricsRunner:
    PROJECT_ID = "ClinEpiDB"

    def __init__(self, user_metrics_url, prometheus_url, env, calendar_month, metrics_writer, acctdb_client):
        """
        :param user_metrics_url: URL of EDA user metrics service, e.g. http://dgaldi.clinepidb.org/eda
        :param prometheus_url: URL of prometheus endpoint, e.g. localhost:9090
        :param env: Environment label to use when filtering prometheus data, e.g. dev, qa or prod
        :param calendar_month: Month in the form yyyy-MM
        """

        self.user_metrics_url = user_metrics_url
        self.prometheus_url = prometheus_url
        self.env = env
        self.metrics_writer = metrics_writer
        self.prometheus_client = PrometheusClient(self.prometheus_url)
        self.acctdb_client = acctdb_client
        if calendar_month == AUTO_PICK_CALENDAR_MONTH:
            current_date = datetime.today()
            last_month_number = 12 if current_date.month == 1 else current_date.month - 1
            last_month_datetime = datetime(year=current_date.year, day=current_date.day, month=last_month_number)
            self.start = first_day_of_month(last_month_datetime)
            self.end = last_day_of_month(last_month_datetime)
        else:
            calendar_month_tokens = calendar_month.split("-")
            self.start = datetime(year=int(calendar_month_tokens[0]), month=int(calendar_month_tokens[1]), day=1)
            self.end = last_day_of_month(self.start)

    def run(self):
        run_id = str(uuid.uuid4())
        self.metrics_writer.create_job(run_id, self.start.month, self.start.year)
        self.handle_analysis_metrics(run_id)
        self.handle_download_metrics(run_id)

    def handle_download_metrics(self, run_id):
        interval = (self.end - self.start).days
        users_to_ignore = self.acctdb_client.query_users_to_ignore() if self.acctdb_client else []
        file_download_metrics = self.prometheus_client.get_metrics(
            'increase(dataset_download_requested_total{environment="' + self.env + '"}[' + str(interval) + 'd])',
            start_date=self.start,
            end_date=self.end,
            labels=['user', 'study'])

        # Filter out zeroes before counting users per study
        file_download_metrics = file_download_metrics[file_download_metrics != 0.0]

        # group dataframe by study and count non-zero users
        file_download_metrics = file_download_metrics.groupby(axis=1, by=lambda x: x[1]).count()
        file_download_metrics = file_download_metrics[~file_download_metrics['user'].isin(users_to_ignore)]
        file_download_metrics = file_download_metrics.transpose().reindex().rename(columns=str)
        file_download_metrics.columns.values[0] = "file_downloads"

        subsetting_download_metrics = self.prometheus_client.get_metrics(
            'increase(subset_download_requested_total{environment="' + self.env + '"}[' + str(interval) + 'd])',
            start_date=self.start,
            end_date=self.end,
            labels=['user_id', 'study_name'])

        subsetting_download_metrics = subsetting_download_metrics[~file_download_metrics['user_id'].isin(users_to_ignore)]
        subsetting_download_metrics = subsetting_download_metrics.groupby(axis=1, by=lambda x: x[1]).count()
        subsetting_download_metrics = subsetting_download_metrics.transpose().reindex().rename(columns=str)
        subsetting_download_metrics.columns.values[0] = "subset_downloads"

        all_download_metrics = file_download_metrics.merge(right=subsetting_download_metrics,
                                                           left_index=True,
                                                           right_index=True,
                                                           how="outer")

        self.metrics_writer.write_downloads_by_study(all_download_metrics, run_id)

    def handle_analysis_metrics(self, run_id):
        user_metrics_client = EdaUserServiceMetricsClient(self.user_metrics_url, self.PROJECT_ID)

        # (analysis_count_bucket, registered_user_count, guest_user_count, registered_user_filters, guest_user_filters)
        analysis_metrics: AnalysisMetrics = user_metrics_client.query_analysis_metrics(self.start, self.end)

        # quantize objects count into buckets to be stored as report output
        per_study_stats_histo = analysis_metrics.user_stats_histogram
        per_study_stats_histo['objects_bucket'] = pd.cut(per_study_stats_histo['objects_count'],
                                                         bins=[-1, 0, 1, 2, 4, 8, 16, 32, 64, sys.maxsize],
                                                         labels=["0", "1", "2", "<=4", "<=8", "<=16", "<=32", "<=64", ">64"])

        # group by new objects count bucket field to produce a histogram and write as output
        bucketed_analysis_histogram = per_study_stats_histo.groupby("objects_bucket").sum()

        # Delete objects count from final output. We have the bucket now
        bucketed_analysis_histogram = bucketed_analysis_histogram.drop(columns="objects_count")

        self.metrics_writer.write_raw_analysis(analysis_metrics.raw_output, run_id)
        self.metrics_writer.write_analysis_histogram(bucketed_analysis_histogram, run_id)
        self.metrics_writer.write_analysis_metrics_by_study(analysis_metrics.study_stats, run_id)
        self.metrics_writer.write_aggregate_stats(analysis_metrics.registered_totals_stats, run_id, "registered")
        self.metrics_writer.write_aggregate_stats(analysis_metrics.guest_totals_stats, run_id, "guest")


def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - timedelta(days=next_month.day)


def first_day_of_month(any_day):
    return any_day.replace(day=1, hour=0, minute=0)
