import json
import sys
from http import client
from urllib.parse import urlparse
import pandas as pd
from usagemetrics.analysis_metrics import AnalysisMetrics


class EdaUserServiceMetricsClient:

    def __init__(self, url, project_id):
        self.url = url
        self.project_id = project_id
        self.base_url = f"/metrics/user/{self.project_id}/analyses"

    def query_analysis_metrics(self, start_date, end_date):
        """
        # Queries the user service and returns a dataframe with a column index on object type and a row index on metric
        # histogram bucket. The cells contain the number of users in each bucket based on how many of the object type they
        # own.

        :param start_date: The start of the range to query.
        :param end_date: The end of the range to query.
        :return:
        """
        eda_url_parse_result = urlparse(self.url)
        if eda_url_parse_result.scheme == 'https':
            eda_client = client.HTTPSConnection(str(eda_url_parse_result.hostname), port=eda_url_parse_result.port)
        else:
            eda_client = client.HTTPConnection(str(eda_url_parse_result.hostname), port=eda_url_parse_result.port)
        # Add this header if using an internal dev or qa site. "Cookie": "auth_tkt=xxx"
        query_start = start_date.isoformat().split('T')[0]
        query_end = end_date.isoformat().split('T')[0]
        url = f"{str(eda_url_parse_result.path)}{self.base_url}?startDate={query_start}&endDate={query_end}"
        eda_client.request(method="GET", url=url, body=None, headers={})
        response = eda_client.getresponse()
        print("Received response with status " + str(response.status))
        if response.status != 200:
            raise RuntimeError("User service did not return a successful response. " + str(response.read()))
        parsed_body = json.loads(response.read())
        # Extract dataframe metrics from output.
        output_df, study_stats = self.extract_dfs(parsed_body)
        # Extract simple dictionaries from output.
        created_or_modified_counts = parsed_body['createdOrModifiedCounts']
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
        return AnalysisMetrics(raw_output=parsed_body,
                               user_stats_histogram=output_df,
                               study_stats=study_stats,
                               registered_totals_stats=registered_totals_stats,
                               guest_totals_stats=guest_totals_stats)

    def extract_dfs(self, response_body):
        created_or_modified_counts = response_body['createdOrModifiedCounts']
        analyses_per_study = pd.DataFrame(created_or_modified_counts["analysesPerStudy"]).rename(
            columns={"studyId": "study_id", "count": "analysis_count"})
        shares_per_study = pd.DataFrame(created_or_modified_counts["importedAnalysesPerStudy"]).rename(
            columns={"studyId": "study_id", "count": "shares_count"})

        study_stats = analyses_per_study.merge(right=shares_per_study, on="study_id", how="outer")

        # Parse different parts of service response into dataframes
        registered_users_histo = self.extract_object_frequencies(response_body, 'registeredUsersAnalysesCounts',
                                                                 'registered_users_with_analysis_count')
        guest_users_histo = self.extract_object_frequencies(response_body, 'guestUsersAnalysesCounts',
                                                            'guest_users_with_analysis_count')
        registered_users_filters_histo = self.extract_object_frequencies(response_body, 'registeredUsersAnalysesCounts',
                                                                         'registered_users_with_filter_count')
        guest_filters_histo = self.extract_object_frequencies(response_body, 'guestUsersFiltersCounts',
                                                              'guest_users_with_filter_count')
        registered_viz_histo = self.extract_object_frequencies(response_body, 'registeredUsersVisualizationsCounts',
                                                               'registered_users_with_viz_count')
        guest_viz_histo = self.extract_object_frequencies(response_body, 'guestUsersVisualizationsCounts',
                                                          'guest_users_with_viz_count')

        output_df = registered_users_histo.merge(
            right=guest_users_histo, how="outer", on="objects_count").merge(
            right=guest_filters_histo, how="outer", on="objects_count").merge(
            right=registered_users_filters_histo, how="outer", on="objects_count").merge(
            right=registered_viz_histo, how="outer", on="objects_count").merge(
            right=guest_viz_histo, how="outer", on="objects_count")

        # TODO: remove the bins, just output entire histogram
        output_df['objects_bucket'] = pd.cut(output_df['objects_count'],
                                             bins=[-1, 0, 1, 2, 4, 8, 16, 32, 64, sys.maxsize],
                                             labels=["0", "1", "2", "<=4", "<=8", "<=16", "<=32", "<=64", ">64"])
        return output_df, study_stats

    @staticmethod
    def extract_object_frequencies(response_body, input_field_name, output_field_name):
        return pd.DataFrame(response_body['createdOrModifiedCounts'][input_field_name]).rename(
            columns={"objectsCount": "objects_count", "usersCount": output_field_name})
