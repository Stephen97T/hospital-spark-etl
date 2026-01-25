from unittest.mock import patch

from pyspark.sql import SparkSession

import main


def test_main_orchestration() -> None:
    """
    Tests that main() correctly initializes Spark and calls run_pipeline
    with the expected environment variables.
    """
    # We mock 'run_pipeline' so it doesn't actually run the ETL during the test
    with patch("main.run_pipeline") as mocked_pipeline:

        # We also mock spark.stop() so we can inspect the session before it dies
        with patch.object(SparkSession, "stop") as mocked_stop:

            # Execute the main function
            main.main()

            # 1. Check if run_pipeline was called once
            assert mocked_pipeline.called

            # 2. Check the arguments passed to run_pipeline
            # call_args[0] returns the positional arguments (spark, env_state, bucket)
            args = mocked_pipeline.call_args[0]

            assert isinstance(args[0], SparkSession)  # Is it a Spark Session?
            assert args[1] == "dev"  # Default ENV_STATE
            assert args[2] == ""  # Default BUCKET_NAME

            # 3. Check if the Spark App Name is correct
            assert args[0].conf.get("spark.app.name") == "HospitalETL"


def test_spark_session_creation() -> None:
    """Verify that Spark session is configured as requested in main.py"""
    with patch("main.run_pipeline"):
        main.main()
        spark = SparkSession.builder.getOrCreate()

        # Check specific configs from your main.py
        assert spark.conf.get("spark.sql.shuffle.partitions") == "4"
        assert spark.conf.get("spark.driver.host") == "127.0.0.1"
