import os
from unittest.mock import patch

from src.kaggle_data import download_and_move_data


@patch("src.kaggle_data.kagglehub.dataset_download")
@patch("src.kaggle_data.os.path.exists")
@patch("src.kaggle_data.os.makedirs")
@patch("src.kaggle_data.os.listdir")
@patch("src.kaggle_data.shutil.copy")
def test_download_and_move_data_success(
    mock_copy, mock_listdir, mock_makedirs, mock_exists, mock_download
):
    # 1. ARRANGE: Set up our fake environment
    mock_download.return_value = "/fake/cache/path"
    mock_exists.return_value = False  # Pretend the 'data' folder doesn't exist yet
    mock_listdir.return_value = ["file1.xlsx", "file2.csv"]  # Fake files from Kaggle

    # 2. ACT: Run the function
    download_and_move_data()

    # 3. ASSERT: Check if the function did the right things
    # Did it call kagglehub with the right dataset name?
    mock_download.assert_called_once_with("t0ut0u/hospital-excel-dataset")

    # Since mock_exists returned False, did it try to create the 'data' directory?
    mock_makedirs.assert_called_once_with("data")

    # Did it try to copy both files to the correct destination?
    assert mock_copy.call_count == 2

    # Verify the first copy call specifically
    # It should join the fake cache path + filename
    expected_source = os.path.join("/fake/cache/path", "file1.xlsx")
    expected_dest = os.path.join("data", "file1.xlsx")
    mock_copy.assert_any_call(expected_source, expected_dest)
