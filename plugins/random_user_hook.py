from airflow.hooks.base import BaseHook
import requests

class RandomUserHook(BaseHook):
    """
    Hook to interact with the Random User API.
    """

    def __init__(self, api_url:str):
        super().__init__()
        self.api_url = api_url

    def get_users(self) -> dict:
        """
        Fetches users from the external API.
        Returns:
            A dictionary representing the JSON response.
        Raises:
            Exception if the API call fails.
        """
        response = requests.get(self.api_url)
        if response.status_code != 200:
            self.log.error(f"Error fetching data: {response.text}")
            raise Exception("Failed to fetch data from Random User API")
        self.log.info("Successfully fetched random users")
        return response.json()