import os
import sys

import usagemetrics.runner as runner
import usagemetrics.metrics_writer as writer


def __main__():
    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} <ENV> <EDA_URL> <PROMETHEUS_URL> <CALENDAR_MONTH|YYYY-MM>")
        exit()

    env = sys.argv[1]
    eda_url = sys.argv[2]
    prometheus_url = sys.argv[3]
    calendar_month = sys.argv[4]

    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    acctdb = os.getenv("ACCTDB")
    ldap_host = os.getenv("LDAP_HOST")
    ldap_query = os.getenv("LDAP_QUERY")

    metrics_writer = writer.MetricsWriter(ldap_host, ldap_query, db_user, db_pass, acctdb)
    runner.UsageMetricsRunner(user_metrics_url=eda_url,
                              prometheus_url=prometheus_url,
                              env=env,
                              calendar_month=calendar_month,
                              metrics_writer=metrics_writer).run()


__main__()
