# Data Exploration and Profiling

This directory contains the analytical phase of the Hospital ETL project. The primary objective of the included notebook is to perform Exploratory Data Analysis (EDA) using PySpark to validate data quality and statistical distribution before moving data into the production pipeline.

## Objectives
* **Schema Validation**: Verify that Spark correctly inferred data types for the 55,500 records.
* **Statistical Profiling**: Analyze categorical frequencies and numerical distributions.
* **Integrity Assessment**: Evaluate relationships between variables (e.g., Age vs. Billing Amount) to determine data authenticity.
* **Logic Prototyping**: Test cleaning functions and feature engineering, such as calculating the Length of Stay (LOS).

## Analysis Summary
The exploration revealed that the dataset is highly balanced, exhibiting a uniform distribution across all categorical features.

| Feature | Observation |
| :--- | :--- |
| **Distributions** | Categorical counts (Medical Condition, Admission Type) show minimal variance. |
| **Correlations** | Pearson Correlation between Age and Billing Amount is near zero (~-0.0038). |
| **Data Source** | Visualizations confirm the data is synthetically generated using randomized uniform sampling. |



## Dependencies and Environment
To execute the notebook, the following environment configuration is required:

1. **Kernel**: The notebook must be run using the project's virtual environment (`.venv`) which contains `pyspark`, `ipykernel`, and `ipywidgets`.
2. **System Path**: The notebook includes a `sys.path.append('..')` call to allow imports from the `/src` directory.
3. **Data Access**: The notebook expects the `hospital-dataset.csv` to be present in the `/data` directory at the project root.

## Visualizations Included
* **Histograms**: Distribution analysis of Age and Billing Amount.
* **Scatter Plots**: Correlation check between patient demographics and financial metrics.
* **Heatmaps**: Cross-tabulation of Admission Types versus Test Results.