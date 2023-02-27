import json
import sys
from http import client
from urllib.parse import urlparse
import pandas
import pandas as pd
from usagemetrics.analysis_metrics import AnalysisMetrics


class EdaUserServiceMetricsClient:

    def __init__(self, url, project_id):
        self.url = url
        self.project_id = project_id

    def query_analysis_metrics(self, start_date, end_date):
        """
        # Queries the user service and returns a dataframe with a column index on object type and a row index on metric
        # histogram bucket. The cells contain the number of users in each bucket based on how many of the object type they
        # own.

        :param start_date:
        :param end_date:
        :return:
        """
        eda_url_parse_result = urlparse(self.url)
        eda_client = client.HTTPConnection(str(eda_url_parse_result.hostname))
        # Add this header if using an internal dev or qa site. "Cookie": "auth_tkt=xxx"
        print(f"URL: {str(eda_url_parse_result.path)}/metrics/user/{self.project_id}/analyses?startDate={start_date.isoformat().split('T')[0]}&endDate={end_date.isoformat().split('T')[0]}")
        eda_client.request(method="GET",
                           url=f"{str(eda_url_parse_result.path)}/metrics/user/{self.project_id}/analyses?startDate={start_date.isoformat().split('T')[0]}&endDate={end_date.isoformat().split('T')[0]}",
                           body=None,
                           headers={})
        response = eda_client.getresponse()
        print("Received response with status " + str(response.status))
        if response.status != 200:
            raise RuntimeError("User service did not return a successful response. " + str(response.read()))

        parsed_body = json.loads(response.read())

        created_or_modified_counts = parsed_body['createdOrModifiedCounts']
        analyses_per_study = pandas.DataFrame(created_or_modified_counts["analysesPerStudy"]).rename(
            columns={"studyId": "study_id", "count": "analysis_count"})
        shares_per_study = pandas.DataFrame(created_or_modified_counts["importedAnalysesPerStudy"]).rename(
            columns={"studyId": "study_id", "count": "shares_count"})
        study_stats = analyses_per_study.merge(right=shares_per_study, on="study_id", how="outer")

        registered_totals_stats = {
            'numUsers': created_or_modified_counts['registeredUsersCount'],
            'numAnalyses': created_or_modified_counts['registeredAnalysesCount'],
            'numFilters': created_or_modified_counts['registeredFiltersCount'],
            'numVisualizations': created_or_modified_counts['registeredVisualizationsCount']
        }

        guest_totals_stats = {
            'numUsers': created_or_modified_counts['guestUsersCount'],
            'numAnalyses': created_or_modified_counts['guestAnalysesCount'],
            'numFilters': created_or_modified_counts['guestFiltersCount'],
            'numVisualizations': created_or_modified_counts['guestVisualizationsCount']
        }

        # Parse different parts of service response into dataframes
        registered_users_histo = pandas.DataFrame(
            parsed_body['createdOrModifiedCounts']['registeredUsersAnalysesCounts']).rename(
            columns={"objectsCount": "objects_count", "usersCount": "registered_users_with_analysis_count"})

        guest_users_histo = pandas.DataFrame(
            parsed_body['createdOrModifiedCounts']['guestUsersAnalysesCounts']).rename(
            columns={"objectsCount": "objects_count", "usersCount": "guest_users_with_analysis_count"})

        guest_filters_histo = pandas.DataFrame(
            parsed_body['createdOrModifiedCounts']['guestUsersFiltersCounts']).rename(
            columns={"objectsCount": "objects_count", "usersCount": "guests_users_with_filter_count"})

        registered_users_filters_histo = pandas.DataFrame(
            parsed_body['createdOrModifiedCounts']['registeredUsersAnalysesCounts']).rename(
            columns={"objectsCount": "objects_count", "usersCount": "registered_users_with_filter_count"})

        # Merge dataframes on objects_count, to create a "histogram table" with all object types.
        output_df = registered_users_histo.merge(right=guest_users_histo, how="outer", on="objects_count")
        output_df = output_df.merge(right=guest_filters_histo, how="outer", on="objects_count")
        output_df = output_df.merge(right=registered_users_filters_histo, how="outer", on="objects_count")

        # TODO: remove the bins, just output entire histogram
        output_df['objects_bucket'] = pd.cut(output_df['objects_count'],
                                             bins=[-1, 0, 1, 2, 4, 8, 16, 32, 64, sys.maxsize],
                                             labels=["0", "1", "2", "<=4", "<=8", "<=16", "<=32", "<=64", ">64"])

        return AnalysisMetrics(raw_output=parsed_body,
                               user_stats_histogram=output_df,
                               study_stats=study_stats,
                               registered_totals_stats=registered_totals_stats,
                               guest_totals_stats=guest_totals_stats)
