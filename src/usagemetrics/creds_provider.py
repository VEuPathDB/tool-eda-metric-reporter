import os
import xml.etree.ElementTree as et


class CredentialsProvider:
    EUPARC = f"{os.path.expanduser('~')}/.euparc"
    ENV_PASS = "DB_PASS"

    def __init__(self, user):
        self.username = user

    def get_db_creds(self, db):
        password = os.getenv(self.ENV_PASS)
        if self.username is None:
            raise EnvironmentError("DB Username must be set in environment variable " + self.username)
        if password is None:
            element_tree = et.parse(self.EUPARC)
            db_element = element_tree.find(f"database/{db}[@login=\"{self.username}\"]")
            password = db_element.get("password")
        if password is None:
            raise EnvironmentError("Unable to parse secret from credentials file.")
        return self.username, password
