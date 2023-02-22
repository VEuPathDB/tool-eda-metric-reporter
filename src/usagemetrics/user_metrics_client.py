import json
import sys
from http import client

import pandas
import pandas as pd


class UserMetricsClient:

    def __init__(self, url, auth_key, project_id):
        self.url = url
        self.auth_key = auth_key
        self.project_id = project_id

    # Queries the user service and returns a dataframe with a column index on object type and a row index on metric
    # histogram bucket. The cells contain the number of users in each bucket based on how many of the object type they
    # own.
    #
    # Columns returned:
    # -objects_bucket
    # -registered_users_with_analysis_count
    # -guest_users_with_analysis_count
    # -guests_users_with_filter_count
    # -registered_users_with_filter_count
    #
    # E.G.
    # objects_bucket    registered_users_with_analysis_count   guests_users_with_filter_count...
    # 0                                       0.0                              0.0...
    # 1                                       7.0                            980.0...
    # 2                                       2.0                            191.0...
    # <=4                                     8.0                             89.0...
    # <=8                                     3.0                             45.0...
    # <=16                                    1.0                             19.0...
    # <=32                                    1.0                              6.0...
    # <=64                                    1.0                              0.0...
    # >64                                     2.0                              0.0...
    def query_analysis_metrics(self, start_date, end_date):
        eda_client = client.HTTPConnection(self.url)
        headers = {"Auth-Key": self.auth_key}
        # Add this header if using an internal dev or qa site. "Cookie": "auth_tkt=xxx"
        eda_client.request(method="GET",
                           url=f"/eda/metrics/user/{self.project_id}/analyses?startDate={start_date.isoformat().split('T')[0]}&endDate={end_date.isoformat().split('T')[0]}",
                           body=None,
                           headers=headers)
        response = eda_client.getresponse()
        print("Received response with status " + str(response.status))
        if response.status != 200:
            raise RuntimeError("User service did not return a successful response. " + str(response.read()))

        parsed_body = json.loads(response.read())

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
        return output_df
