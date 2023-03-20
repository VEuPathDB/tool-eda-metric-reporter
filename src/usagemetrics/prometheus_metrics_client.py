import json

import pandas as pd
from http import client


class PrometheusClient:

    def __init__(self, base_url):
        self.base_url = base_url

    def get_metrics(self, query, start_date, end_date, labels):
        print("Executing query: " + query)
        interval = (end_date - start_date).days
        url = f'/api/v1/query_range?query={query}&start={start_date.isoformat() + "Z"}&end={end_date.isoformat() + "Z"}&step={str(interval)}d'
        prom_client = client.HTTPConnection(self.base_url)
        prom_client.request(method="GET", url=url)
        response = prom_client.getresponse()
        if response.status != 200:
            raise RuntimeError("Prometheus service did not return a successful response. " + str(response.read()))
        df = self.parse_to_dataframe(response, labels)
        return df

    @staticmethod
    def parse_to_dataframe(response, labels):
        """
        Takes a response from the prometheus client and returns a dataframe with a column multi index on the provided
        labels and a row index of data point times.
        E.G:
                   AVENIR-1                JILCOST-1
                  354383910   354383910    454383915
        2023-02-17       0.0  2.019454       1.0
        2023-02-18       0.0  0.000000       1.0

        :param response: Dataframe containing metrics.
        :param labels: Labels to hierarchically index on.
        :return:
        """
        parsed_response = json.loads(response.read())
        result = parsed_response['data']['result']
        df = pd.DataFrame({tuple([r['metric'][label] for label in labels]):
                          pd.Series(
                              (float(v[1]) for v in r['values']),
                              index=(pd.Timestamp(v[0], unit='s') for v in r['values'])) for r in result})
        return df
