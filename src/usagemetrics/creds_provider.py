import os
import xml.etree.ElementTree as et


class CredentialsProvider:
    ENV_PASS = "DB_PASS"

    def __init__(self, user, secrets_file):
        self.username = user
        self.secrets_file = secrets_file

    def get_db_creds(self, db):
        password = os.getenv(self.ENV_PASS)
        if self.username is None:
            raise EnvironmentError("DB Username must be set in environment variable " + self.username)
        if password is None:
            print(f"Looking for username {self.username} in file {self.secrets_file}")
            element_tree = et.parse(self.secrets_file)
            db_element = element_tree.find(f"database/user[@login=\"{self.username}\"]")
            password = db_element.get("password")
        if password is None:
            raise EnvironmentError("Unable to parse secret from credentials file.")
        return self.username, password
