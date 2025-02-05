from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from random_user_hook import RandomUserHook
import pandas as pd

class RandomUserOperator(BaseOperator):
     
     """
    Operator to fetch 500 random users from the external API.
    """
     @apply_defaults
     def __init__(
         self, 
         api_url:str = 'https://randomuser.me/api/?results=50',
         output_path: str = "/tmp/random_users.csv", 
         *args,
         **kwargs
         ):
          super().__init__(*args, **kwargs)
          self.api_url = api_url
          self.output_path = output_path

     def execute(self, context):  
        # Instantiate the hook with the provided API URL
        hook : RandomUserHook = RandomUserHook(api_url=self.api_url)
        # Fetch the users
        data = hook.get_users()
        # Log the number of users fetched or any additional info
        results = data.get("results", [])
        self.log.info(f"Fetched {len(results)} users from the API.")

        # Extract desired fields into a list of dictionaries.
        records = []
        for user in results:
            record = {
                "gender": user.get("gender", ""),
                "title": user.get("name", {}).get("title", ""),
                "first_name": user.get("name", {}).get("first", ""),
                "last_name": user.get("name", {}).get("last", ""),
                "email": user.get("email", ""),
                "dob_date": user.get("dob", {}).get("date", ""),
                "dob_age": user.get("dob", {}).get("age", ""),
                "phone": user.get("phone", ""),
                "cell": user.get("cell", ""),
                "nat": user.get("nat", "")
            }
            records.append(record)

        df = pd.DataFrame(records)
        df.to_csv(self.output_path, index=False)
        self.log.info(f"CSV file successfully saved to {self.output_path}")
        
        return self.output_path