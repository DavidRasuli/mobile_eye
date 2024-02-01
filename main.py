import logging

from athena_query_wrapper import AthenaQueryWrapper
from distribution_percentage_query_builder import DistributionPercentageQueryBuilder
from proof_of_protability.simple_select_query_builder import SelectQueryBuilder

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    """
        Main entry point for the script.
        
        This acts as integration test

        Initializes the AthenaQueryWrapper and DistributionPercentageQueryBuilder,
        adds distribution ranges, builds and runs an Athena query, and processes the result.
    """
    try:
        wrapper = AthenaQueryWrapper(
            database='detection_db',
            output_bucket='me-interview',
            output_folder='results',
            region='eu-north-1',
            aws_access_key_id='key',
            aws_secret_access_key='secret'
        )

        query_builder = DistributionPercentageQueryBuilder(
            table='distance_detection',
            distribution_variable='vehicle_type',
            comparison_variable='detection',
            range_field='distance'
        )

        query_builder.add_distribution_range(0, 10)
        query_builder.add_distribution_range(10, 20)
        query_builder.add_distribution_range(20, 100)
        query = query_builder.build()

        logger.info(f"About to run the following query: {query}")

        result = wrapper.run_query(query)

        print(result)

        # Let's prove portability
        select_builder = SelectQueryBuilder(table='distance_detection')

        # Adding columns dynamically
        select_builder.add_column('clip_name')
        select_builder.add_column('frame_id')
        select_builder.add_column('vehicle_type')

        # Building the SELECT query
        select_query = select_builder.build()

        logger.info(f"Generated SELECT query: {select_query}")

        logger.info(f"About to run the following query: {query}")

        result = wrapper.run_query(select_query)

        print(result)

    except Exception as e:
        logger.error(f"Error in main.py: {str(e)}")
