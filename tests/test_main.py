from unittest.mock import patch, MagicMock

from pyspark.sql import SparkSession

import main


@patch.object(SparkSession, "stop")
@patch("main.run_pipeline")
def test_main_orchestration(
    mock_run_pipeline: MagicMock, mock_spark_stop: MagicMock
) -> None:
    """Tests that main() correctly initializes Spark and calls run_pipeline
    with the expected environment variables.
    """
    # Execute the main function
    main.main()

    # 1. Check if run_pipeline was called once
    assert mock_run_pipeline.called

    # 2. Check the arguments passed to run_pipeline
    # call_args[0] returns the positional arguments (spark, env_state, bucket)
    args = mock_run_pipeline.call_args[0]

    assert isinstance(args[0], SparkSession)  # Is it a Spark Session?
    assert args[1] == "dev"  # Default ENV_STATE
    assert args[2] == ""  # Default BUCKET_NAME

    # 3. Check if the Spark App Name is correct
    assert args[0].conf.get("spark.app.name") == "HospitalETL"


@patch("pyspark.sql.SparkSession.builder")
@patch("main.run_pipeline")
def test_spark_session_creation(
    mock_run_pipeline: MagicMock, mock_builder: MagicMock
) -> None:
    """Verify that Spark session is configured as requested in main.py"""
    mock_spark = MagicMock()
    # Mock the builder chain
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_spark

    main.main()

    # Check that the configs were set as expected
    mock_builder.appName.assert_called_with("HospitalETL")
    mock_builder.master.assert_called_with("local[*]")
    mock_builder.config.assert_any_call("spark.sql.shuffle.partitions", "4")
    mock_builder.config.assert_any_call("spark.driver.host", "127.0.0.1")
    mock_builder.getOrCreate.assert_called_once()
