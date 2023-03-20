import os
import xml.etree.ElementTree as et


class CredentialsProvider:
    EUPARC = f"{os.path.expanduser('~')}/.euparc"
    ENV_PASS = "DB_PASS"
    ENV_USER = "DB_USER"

    def get_db_creds(self, db):
        password = os.getenv(self.ENV_PASS)
        username = os.getenv(self.ENV_USER)
        if username is None:
            raise EnvironmentError("DB Username must be set in environment variable " + self.ENV_USER)
        if password is None:
            element_tree = et.parse(self.EUPARC)
            db_element = element_tree.find(f"database/{db}[@login=\"{username}\"]")
            password = db_element.get("password")
        if password is None:
            raise EnvironmentError("Unable to parse secret from credentials file.")
        return username, password
