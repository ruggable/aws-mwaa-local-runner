from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import boto3
import time


class AwsGlueCrawlerOperator(BaseOperator):
    """
    An operator that runs a Glue crawler using boto3.

    :param crawler_name: The name of the Glue crawler to run.
    """
    @apply_defaults
    def __init__(
            self,
            crawler_name: str,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.crawler_name = crawler_name

    def execute(self, context):

        glue_client = boto3.client('glue')

        # Get current state of the crawler
        crawler_info = glue_client.get_crawler(Name=self.crawler_name)

        if crawler_info["Crawler"]["State"] == 'RUNNING':
            self.log.info(f'Glue crawler {self.crawler_name} is already RUNNING. Waiting for completion...')
        else:
            # Start the crawler
            try:
                glue_client.start_crawler(Name=self.crawler_name)
                self.log.info(f'Starting Glue crawler {self.crawler_name}.')
            except AirflowException as e:
                self.log.error(f'Unable to start Glue crawler {self.name}.')
                raise e

        while crawler_info:

            crawler_info = glue_client.get_crawler(Name=self.crawler_name)

            if crawler_info["Crawler"]["State"] not in ["READY", "STOPPING", "RUNNING"]:
                self.log.error(f'Glue Crawler {self.crawler_name} failed in unknown state')
                raise AirflowException(f'Glue Crawler {self.crawler_name} is in an unknown state, marking failed.')

            if crawler_info["Crawler"]["State"] in ['STOPPING', 'READY']:
                self.log.info(f'Glue crawler {self.crawler_name} done.')
                return 'succeeded'

            # If RUNNING sleep and then check again
            elif crawler_info["Crawler"]["State"] == 'RUNNING':
                self.log.info(f'Glue crawler {self.crawler_name} is {crawler_info["Crawler"]["State"]}. '
                              f'Waiting...')
                time.sleep(5)
