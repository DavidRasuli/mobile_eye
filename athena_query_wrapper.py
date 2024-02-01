import time

import logging
import boto3

logger = logging.getLogger(__name__)


class AthenaQueryWrapper:
    def __init__(self, database, output_bucket, output_folder, aws_access_key_id='YOUR_ACCESS_KEY',
                 aws_secret_access_key='YOUR_SECRET_KEY', region='eu-north-1',
                 max_attempts=60, sleep_time=5):
        self._database = database
        self._output_bucket = output_bucket
        self._output_folder = output_folder
        self._athena_client = boto3.client('athena',
                                           region_name=region,
                                           aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key
                                           )
        self._max_attempts = max_attempts
        self._sleep_time = sleep_time

        """
       Initializes an instance of AthenaQueryWrapper.

       Parameters:
       - database (str): The Athena database name.
       - output_bucket (str): The S3 bucket for query results.
       - output_folder (str): The folder within the S3 bucket for query results.
       - aws_access_key_id (str): AWS access key ID.
       - aws_secret_access_key (str): AWS secret access key.
       - region (str): AWS region.
       - max_attempts (int): Maximum attempts for checking query status.
       - sleep_time (int): Sleep time (in seconds) between query status checks.
       """

    def run_query(self, query_statement: str) -> dict:
        """
        Runs an Athena query.

        Parameters:
        - query_statement (str): The Athena SQL query statement.

        Returns:
        - dict: The result of the Athena query.
        """
        try:
            query_execution_id = self._start_query_execution(query_statement)
            query_result = self._get_query_results(query_execution_id)
            return query_result
        except Exception as e:
            logger.error(f"Error running Athena query: {str(e)}")
            raise  # Re-raise the exception to propagate it up the call stack

    def _start_query_execution(self, query_statement) -> str:
        """
        Starts the execution of an Athena query.

        Parameters:
        - query_statement (str): The Athena SQL query statement.

        Returns:
        - str: The query execution ID.
        """
        try:
            response = self._athena_client.start_query_execution(
                QueryString=query_statement,
                QueryExecutionContext={'Database': self._database},
                ResultConfiguration={
                    'OutputLocation': f's3://{self._output_bucket}/{self._output_folder}/'
                }
            )
            return response['QueryExecutionId']
        except Exception as e:
            logger.error(f"Error starting Athena query execution: {str(e)}")
            raise  # Re-raise the exception to propagate it up the call stack

    def _get_query_results(self, query_execution_id):
        """
        Gets the results of a completed Athena query.

        Parameters:
        - query_execution_id (str): The ID of the executed Athena query.

        Returns:
        - dict: The result of the Athena query.
        """

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                query_state = response['QueryExecution']['Status']['State']

                if query_state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break

                time.sleep(self._sleep_time)
            except Exception as e:
                logger.warning(f"Error checking Athena query status (attempt {attempt}): {str(e)}")
                if attempt == self._max_attempts:
                    raise  # If max attempts reached, re-raise the exception

        try:
            query_result = self._athena_client.get_query_results(QueryExecutionId=query_execution_id)
            return query_result
        except Exception as e:
            logger.error(f"Error getting Athena query results: {str(e)}")
            raise  # Re-raise the exception to propagate it up the call stack
