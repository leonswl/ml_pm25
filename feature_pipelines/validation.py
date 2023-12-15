"""Python script to build Expectation Suite"""
from great_expectations.core import ExpectationSuite, ExpectationConfiguration

def build_expectation_suite() -> ExpectationSuite:
    """
    Builder used to retrieve an instance of the validation expectation suite.
    """

    expectation_suite_pm25 = ExpectationSuite(
        expectation_suite_name="pm25_suite"
    )

    ## Columns

    # ordered list
    expectation_suite_pm25.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_columns_to_match_ordered_list",
            kwargs={
                "column_list": [
                    "timestamp",
                    "update_timestamp",
                    "reading_west",
                    "reading_east",
                    "reading_central",
                    "reading_south",
                    "reading_north",
                ]
            },
        )
    )

    # total columns count
    expectation_suite_pm25.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_column_count_to_equal", kwargs={"value": 7}
        )
    )

    ## timestamp

    # not null
    expectation_suite_pm25.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "timestamp"},
        )
    )

    ## update_timestamp

    # not null
    expectation_suite_pm25.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "update_timestamp"},
        )
    )

    ## readings
    readings_list = ["reading_west","reading_east","reading_central","reading_north",'reading_south']

    for reading in readings_list:
        # is int
        expectation_suite_pm25.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={
                "column": reading,
                "type_": "int32",
                "min_value": 0,
                "max_value": 500
                },
        )

        # is not null
        
    )
    
