from unittest.mock import patch, MagicMock, PropertyMock

from src.pipeline import run_pipeline


@patch("src.pipeline.download_and_move_data")
@patch("src.pipeline.prepare_hospital_data")
@patch("src.pipeline.upload_local_to_gcs")
def test_run_pipeline_flow_dev(
    mock_upload, mock_prepare, mock_download, spark_session
) -> None:
    # 1. Setup - Create a real DF to be returned by our mock
    df_input = spark_session.createDataFrame(
        [("JOHN DOE", "2023-01-01", "2023-01-10", 50000, "Cancer")],
        [
            "Name",
            "Date of Admission",
            "Discharge Date",
            "Billing Amount",
            "Medical Condition",
        ],
    )

    # 2. Patch the READ property on the SparkSession CLASS
    with patch(
        "pyspark.sql.SparkSession.read", new_callable=PropertyMock
    ) as mock_read_prop:
        mock_reader = MagicMock()
        mock_read_prop.return_value = mock_reader
        # When .csv() is called, return our real DataFrame
        mock_reader.csv.return_value = df_input

        # 3. Patch the WRITE property on the DataFrame CLASS
        with patch(
            "pyspark.sql.DataFrame.write", new_callable=PropertyMock
        ) as mock_write_prop:
            mock_writer = MagicMock()
            mock_write_prop.return_value = mock_writer

            # Handle the chained write methods
            mock_writer.mode.return_value = mock_writer
            mock_writer.partitionBy.return_value = mock_writer
            mock_writer.parquet.return_value = None

            # Execute
            run_pipeline(spark_session, env_state="dev", bucket_name="test-bucket")

            # 4. Assertions
            mock_download.assert_called_once()
            mock_upload.assert_not_called()  # No upload in DEV
            mock_writer.parquet.assert_called_once_with("data/gold_hospital_data")


@patch("src.pipeline.download_and_move_data")
@patch("src.pipeline.prepare_hospital_data")
@patch("src.pipeline.upload_local_to_gcs")
def test_run_pipeline_flow_prod(
    mock_upload, mock_prepare, mock_download, spark_session
) -> None:
    df_input = spark_session.createDataFrame(
        [("Alice", "2023-01-01", "2023-01-05", 100, "Flu")],
        [
            "Name",
            "Date of Admission",
            "Discharge Date",
            "Billing Amount",
            "Medical Condition",
        ],
    )

    with patch(
        "pyspark.sql.SparkSession.read", new_callable=PropertyMock
    ) as mock_read_prop:
        mock_reader = MagicMock()
        mock_read_prop.return_value = mock_reader
        mock_reader.csv.return_value = df_input

        with patch(
            "pyspark.sql.DataFrame.write", new_callable=PropertyMock
        ) as mock_write_prop:
            mock_writer = MagicMock()
            mock_write_prop.return_value = mock_writer
            mock_writer.mode.return_value = mock_writer
            mock_writer.partitionBy.return_value = mock_writer

            # Run in PROD
            run_pipeline(spark_session, env_state="prod", bucket_name="my-real-bucket")

            # Verify GCS Upload was triggered
            mock_upload.assert_called_once_with(
                "data/gold_hospital_data", "my-real-bucket", "gold_hospital_data"
            )
