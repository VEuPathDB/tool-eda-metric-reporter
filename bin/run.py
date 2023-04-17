import json
import os
import sys

import usagemetrics.runner as runner
import usagemetrics.metrics_writer as writer
import usagemetrics.creds_provider as creds
import usagemetrics.acctdb_client as acctdb

def __main__():
    if len(sys.argv) != 12:
        print(f"Usage: {sys.argv[0]} <ENV> <EDA_URL> <PROMETHEUS_URL> <CALENDAR_MONTH|YYYY-MM> <TARGET_DB> <LDAP_HOST> <LDAP_QUERY> <USER> <SECRETS_FILE> <PROJECT_ID> <QA_AUTH_FILE>")
        exit()

    env = sys.argv[1]
    eda_url = sys.argv[2]
    prometheus_url = sys.argv[3]
    calendar_month = sys.argv[4]
    target_db = sys.argv[5]

    ldap_host = sys.argv[6]
    ldap_query = sys.argv[7]

    db_user, db_pass = creds.CredentialsProvider(sys.argv[8], sys.argv[9]).get_db_creds(target_db)
    project_id = sys.argv[10]

    qa_auth_file = sys.argv[11]

    metrics_writer = writer.MetricsWriter(ldap_host, ldap_query, db_user, db_pass, target_db)
    acctdb_client = acctdb.AccountDbClient(ldap_host, ldap_query, db_user, db_pass, "acctdbn")

    runner.UsageMetricsRunner(user_metrics_url=eda_url,
                              prometheus_url=prometheus_url,
                              env=env,
                              calendar_month=calendar_month,
                              metrics_writer=metrics_writer,
                              acctdb_client=acctdb_client,
                              project_id=project_id,
                              qa_auth_file=qa_auth_file).run()


__main__()
