import os
from unittest.mock import patch, MagicMock

import pandas as pd

from src.kaggle_data import download_and_move_data, prepare_hospital_data


@patch("src.kaggle_data.kagglehub.dataset_download")
@patch("src.kaggle_data.os.path.exists")
@patch("src.kaggle_data.os.makedirs")
@patch("src.kaggle_data.os.listdir")
@patch("src.kaggle_data.shutil.copy")
def test_download_and_move_data_success(
    mock_copy: MagicMock,
    mock_listdir: MagicMock,
    mock_makedirs: MagicMock,
    mock_exists: MagicMock,
    mock_download: MagicMock,
) -> None:
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


@patch("src.kaggle_data.os.path.exists")
@patch("src.kaggle_data.pd.read_excel")
@patch("src.kaggle_data.pd.DataFrame.to_csv")
def test_prepare_hospital_data(
    mock_to_csv: MagicMock, mock_read_excel: MagicMock, mock_exists: MagicMock
) -> None:
    # 1. ARRANGE: Set up mock behavior
    excel_path = "data/hospital-dataset.xlsx"
    csv_path = "data/hospital-dataset.csv"

    # Mock file existence checks
    mock_exists.side_effect = lambda path: path == excel_path

    # Mock pandas read_excel and to_csv
    mock_read_excel.return_value = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

    # 2. ACT: Call the function
    result = prepare_hospital_data(excel_path)

    # 3. ASSERT: Verify behavior
    assert result == csv_path  # Ensure the correct CSV path is returned
    mock_read_excel.assert_called_once_with(excel_path)  # Ensure Excel was read
    mock_to_csv.assert_called_once_with(csv_path, index=False)  # Ensure CSV was written
