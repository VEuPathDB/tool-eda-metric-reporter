import os
import sys

import usagemetrics.runner as runner


def __main__():
    auth_key = os.environ.get('AUTH_KEY')

    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <ENV> <EDA_URL> <PROMETHEUS_URL> <CALENDAR_MONTH|YYYY-MM>")

    env = sys.argv[1]
    eda_url = sys.argv[2]
    prometheus_url = sys.argv[3]
    calendar_month = sys.argv[4]

    runner.UsageMetricsRunner(user_metrics_url=eda_url,
                              auth_key=auth_key,
                              prometheus_url=prometheus_url,
                              env=env,
                              calendar_month=calendar_month).run()


__main__()
