import unittest
from distribution_percentage_query_builder import DistributionPercentageQueryBuilder


class TestDistributionPercentageQueryBuilder(unittest.TestCase):
    def setUp(self):
        self.query_builder = DistributionPercentageQueryBuilder(
            table='test_table',
            distribution_variable='test_distribution',
            comparison_variable='test_comparison',
            range_field='test_range'
        )

    def test_add_distribution_range(self):
        self.query_builder.add_distribution_range(0, 10)
        self.assertEqual(len(self.query_builder._columns), 1)

    def test_build_with_no_ranges(self):
        with self.assertRaises(ValueError):
            self.query_builder.build()

    def test_build_with_ranges(self):
        self.query_builder.add_distribution_range(0, 10)
        self.query_builder.add_distribution_range(10, 20)
        query = self.query_builder.build()

        self.assertIn('SELECT', query)
        self.assertIn('test_distribution', query)
        self.assertIn('range_0_10', query)
        self.assertIn('range_10_20', query)

    def test_add_invalid_distribution_range(self):
        with self.assertRaises(ValueError):
            self.query_builder.add_distribution_range(10, 5)


if __name__ == '__main__':
    unittest.main()

