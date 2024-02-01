class DistributionPercentageQueryBuilder:
    def __init__(self, table, distribution_variable, comparison_variable, range_field):
        """
        Initializes an instance of DistributionPercentageQueryBuilder.

        Parameters:
        - table (str): The table name.
        - distribution_variable (str): The variable for distribution in the query.
        - range_field (str): The variable which will create the bins (columns) in the query
        - comparison_variable (str): The variable for comparison in the query - must be record of type bool
        """
        self._table = table
        self._range_field = range_field
        self._comparison_field = comparison_variable
        self._distribution_field = distribution_variable
        self._columns = []

    def add_distribution_range(self, start_range, end_range):
        """
       Adds a distribution range to the query.

       Parameters:
       - start_range (int): The start of the distribution range.
       - end_range (int): The end of the distribution range.
       """
        if not (isinstance(start_range, int) and isinstance(end_range, int) and start_range <= end_range <= 100):
            raise ValueError("Invalid input for distribution range. Start and end range should be integers between 1 "
                             "and 100.")

        column_name = f"range_{start_range}_{end_range}"
        case_statement = f"100.0 * AVG(CASE WHEN {self._range_field} BETWEEN {start_range} AND {end_range} AND {self._comparison_field} THEN 1 ELSE 0 END) AS {column_name}"
        self._columns.append(case_statement)

    def build(self) -> str:
        """
        Builds the complete query based on added distribution ranges.

        Returns:
        - str: The complete query.
        """
        if not self._columns:
            raise ValueError("No distribution ranges added. Please use add_distribution_range before building the "
                             "query.")

        select_clause = f"SELECT {self._distribution_field}, {', '.join(self._columns)} FROM {self._table}"
        group_by_clause = f" GROUP BY {self._distribution_field}"
        return f"{select_clause}{group_by_clause}"
