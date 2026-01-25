from unittest.mock import MagicMock, patch

from src.cleanse_data import apply_feature_engineering
from src.cleanse_data import clean_columns_names


def test_clean_columns_names_logic() -> None:
    """
    Verifies that 'clean_columns_names' correctly transforms
    column names and calls toDF with the results.
    """
    # 1. Setup Mock DataFrame
    mock_df = MagicMock()

    # Simulate the .columns attribute of a Spark DataFrame
    mock_df.columns = ["Patient Name", "DATE of ADMISSION", "billing amount"]

    # Ensure .toDF returns the mock itself (for chaining compatibility)
    mock_df.toDF.return_value = mock_df

    # 2. Execute the function
    result_df = clean_columns_names(mock_df)

    # 3. Assertions
    expected_names = ["patient_name", "date_of_admission", "billing_amount"]

    # Verify toDF was called with exactly the names we expected
    mock_df.toDF.assert_called_once_with(*expected_names)

    # Verify the function returned the transformed dataframe
    assert result_df == mock_df


@patch("src.cleanse_data.F")
def test_apply_feature_engineering_sparkless(mock_f) -> None:
    """
    Verifies the transformation logic without triggering any JVM/Spark calls,
    handling operators like '>' correctly.
    """
    # 1. Setup the Mock DataFrame
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df

    # 2. Create a "Smart Column" Mock
    # This mock will return itself when compared (> 30000) or called
    smart_col = MagicMock()
    smart_col.__gt__.return_value = smart_col  # Handles the '>' operator

    # 3. Setup the Mock Functions
    mock_f.col.return_value = smart_col
    mock_f.lower.return_value = smart_col
    mock_f.initcap.return_value = smart_col
    mock_f.datediff.return_value = smart_col

    # Handle the chained .when().otherwise()
    mock_f.when.return_value = smart_col
    smart_col.otherwise.return_value = smart_col

    # 3. Execute
    apply_feature_engineering(mock_df)

    # 4. Assertions
    actual_col_names = [call.args[0] for call in mock_df.withColumn.call_args_list]

    assert "name" in actual_col_names
    assert "stay_duration" in actual_col_names
    assert "is_high_bill" in actual_col_names
    assert mock_df.withColumn.call_count == 3
