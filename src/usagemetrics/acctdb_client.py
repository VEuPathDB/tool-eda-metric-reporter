import re
import subprocess
import cx_Oracle


class AccountDbClient:

    def __init__(self, ldap_host, ldap_query, username, password, acctdb):
        print(f"Connect with {ldap_host} {ldap_query}")
        # Query LDAP for db connect information.
        args = ['ldapsearch', '-x', '-H', f"ldaps://{ldap_host}", '-b', ldap_query, '-s', 'sub', f"(&(cn={acctdb})(objectClass=orclNetService))", 'orclNetDescString']
        print("Running command with args: " + str(args))
        output = subprocess.check_output(args).decode('utf-8').replace("\n", '').replace(" ", "")
        match = re.search("HOST=([A-Za-z0-9.]+).*PORT=([0-9]+).*SERVICE_NAME=([a-zA-Z0-9.]+)", output)
        host = match.group(1)
        port = match.group(2)
        service = match.group(3)

        # Construct db connect information with parsed details from LDAP.
        self.connection = cx_Oracle.connect(
            user=username,
            password=password,
            dsn=f"{host}:{port}/{service}",
            encoding='utf-8')

    def query_users_to_ignore(self):
        sql = '''
          select user_id from useraccounts.account_properties
          where key = 'ignore_in_metrics' and value = 'true'
          '''
        cursor = self.connection.cursor()
        return [row[0] for row in cursor.execute(sql)]




