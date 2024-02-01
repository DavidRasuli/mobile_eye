# Big Data Analytics team – Technical interview

As part of interview with Mobile Eye, this is a task involves usage of S3, Athena, Glue, and python. 

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Files and Structure](#files-and-structure)
- [Dependencies](#dependencies)
- [Assumptions](#assumptions)
- [Tests](#tests)
- [Portability](#portability)
- [Extensibility](#extensibility)

## Overview

- **Athena Query Wrapper**: A class (`athena_query_wrapper.py`) that serves as a wrapper for executing queries on Amazon Athena.

- **Distribution Percentage Query Builder**: A query builder (`distribution_percentage_query_builder.py`) tailored for the task requirement - distribution percentage queries. It enables dynamic generation of queries to analyze distribution percentages based on specified ranges.

- **Select Query Builder**: Another query builder (`select_query_builder.py`) focused on creating SELECT queries dynamically. It allows users to add columns and build SELECT queries with ease.



## Getting Started

Assume python 3.8 or later 
pip install -r requirements.txt

## Usage

See main.py - it acts as a smoke test - and as such tests the athena_query_wrapper.py.

# Files and Structure

Overview of the project structure and the purpose of each file:

- main.py: Entry point of the application, demonstrating the usage of query builders - this serves as smoke test.

- athena_query_wrapper.py: Wrapper class for executing Athena queries.

- distribution_percentage_query_builder.py: Query builder for creating distribution percentage queries, as instructed by the task itself.

tests folder (below)

proof of portability folder (below)

# Dependencies
List of dependencies needed to run the project.

boto3: AWS SDK for Python.
botocore: boto stub for testing.

# Assumptions

- The SDK user could be unaware of SQL syntax and usage, but does know the table name and basic structure, including types.
- The query builders are as portable as possible within the scope of this assignment - I've tried not to over-engineer it, yet use OOP and OOD concepts to show knowledge.
- I did put effort and time for this task, but tried to handle and deliver it as if I would spend an entire day for it.
- Obviously, such task can take many different directions, having said that, I'll be willing to make any necessary adjustments to have it more polished.

# Tests

Under the folder /tests, there are unit tests for both the query builder, no need to set aws credentials.

# Portability

select_query_builder.py: Query builder for creating SELECT queries. Introduced it to prove portability.
See usage in main.py

# Extensibility

- With the structure and tests that already covered at distribution_percentage_query_builder.py, it would be relatively easy to add an aggregation function injected at the constructor, and have it more flexible than it is

for example :

    class DistributionPercentageQueryBuilderWithAggregator...:
        def __init__:
            ...
            aggregator: str # avg count etc
            ...
        ...
        def add_distribution_range(self, start_range, end_range):
            ...
            case_statement = f"100.0 * {aggregator}(CASE WHEN ...
            ...
        ...


