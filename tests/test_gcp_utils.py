import os
from unittest.mock import patch, MagicMock

from src.gcp_utils import upload_local_to_gcs


@patch("src.gcp_utils.storage.Client")
def test_upload_local_to_gcs(mock_storage_client, tmp_path) -> None:
    """
    Verifies that the upload function:
    1. Walks the local directory
    2. Identifies all files
    3. Calls the GCS 'upload_from_filename' with the correct paths
    """
    # 1. Create a dummy local directory structure using tmp_path (pytest fixture)
    local_dir = tmp_path / "data_folder"
    local_dir.mkdir()
    file1 = local_dir / "file1.parquet"
    file2 = local_dir / "file2.parquet"
    file1.write_text("content1")
    file2.write_text("content2")

    # 2. Setup the Mocks
    mock_client_instance = mock_storage_client.return_value
    mock_bucket = MagicMock()
    mock_blob = MagicMock()

    mock_client_instance.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    # 3. Execute the function
    bucket_name = "my-test-bucket"
    gcs_destination = "gold_data"
    upload_local_to_gcs(str(local_dir), bucket_name, gcs_destination)

    # 4. Assertions
    # Did it initialize the client and get the right bucket?
    mock_storage_client.assert_called_once()
    mock_client_instance.bucket.assert_called_with(bucket_name)

    # Did it try to upload exactly 2 files?
    assert mock_blob.upload_from_filename.call_count == 2

    # Verify the GCS paths were constructed correctly (relative paths)
    # The blob paths should look like "gold_data/file1.parquet"
    expected_calls = [
        os.path.join(gcs_destination, "file1.parquet"),
        os.path.join(gcs_destination, "file2.parquet"),
    ]

    # Get all the blob paths that were actually created
    actual_blob_paths = [call.args[0] for call in mock_bucket.blob.call_args_list]

    # We sort because os.walk order can vary by OS
    assert sorted(actual_blob_paths) == sorted(expected_calls)
