import os
import sys

import usagemetrics.runner as runner
import usagemetrics.metrics_writer as writer
import usagemetrics.creds_provider as creds


def __main__():
    if len(sys.argv) != 6:
        print(f"Usage: {sys.argv[0]} <ENV> <EDA_URL> <PROMETHEUS_URL> <CALENDAR_MONTH|YYYY-MM> <TARGET_DB>")
        exit()

    env = sys.argv[1]
    eda_url = sys.argv[2]
    prometheus_url = sys.argv[3]
    calendar_month = sys.argv[4]
    target_db = sys.argv[5]

    ldap_host = os.getenv("LDAP_HOST")
    ldap_query = os.getenv("LDAP_QUERY")

    db_user, db_pass = creds.CredentialsProvider().get_db_creds(target_db)

    metrics_writer = writer.MetricsWriter(ldap_host, ldap_query, db_user, db_pass, target_db)
    runner.UsageMetricsRunner(user_metrics_url=eda_url,
                              prometheus_url=prometheus_url,
                              env=env,
                              calendar_month=calendar_month,
                              metrics_writer=metrics_writer).run()


__main__()
