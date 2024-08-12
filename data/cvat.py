import os
from dotenv import load_dotenv
from cvat_sdk import Client
from cvat_sdk import make_client
from cvat_sdk.api_client import Configuration


class CVATConnection:
    def __init__(self):
        load_dotenv()
        self.base_url = os.getenv("CVAT_HOST")
        self.username = os.getenv("CVAT_USERNAME")
        self.password = os.getenv("CVAT_PASSWORD")
        self.org_slug = os.getenv("CVAT_ORG_SLUG")
        self.cvat_client = self.create_client()
        self.config = self.create_config()

    def create_client(self) -> Client:
        try:
            self.cvat_client = make_client(
                credentials=(
                    self.username,
                    self.password,
                ),
                host=self.base_url,
            )
        except KeyError as e:
            raise RuntimeError(f'Please, specify {e.args} in env vars.')
        self.cvat_client.organization_slug = self.org_slug
        return self.cvat_client
    
    def create_config(self) -> Configuration:
        return Configuration(
            host=self.base_url,
            username=self.username,
            password=self.password
        )