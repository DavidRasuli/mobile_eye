
'''
The idea behind this class is to prove that the query builder
structure can work well for other objects for other
purposes within this solution. Limiting to 10 records - hardcoded.
'''


class SelectQueryBuilder:
    def __init__(self, table):
        self._table = table
        self._columns = []

    def add_column(self, column):
        self._columns.append(column)

    def build(self) -> str:
        if not self._columns:
            raise ValueError("No columns added. Please use add_column before building the query.")

        select_clause = f"SELECT {', '.join(self._columns)} FROM {self._table} limit 10"
        return select_clause
