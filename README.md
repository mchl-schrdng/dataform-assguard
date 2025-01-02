# dataform-qaguard

This repository is a **personal side project** aimed at monitoring and analyzing the quality of assertions in Dataform workflows using Google Cloud Platform (GCP) services. The project focuses on automating the process of fetching, storing, and visualizing assertion-related data for improved insights and quality control.

## Project Overview

This tool performs the following tasks:

1. **Authentication**: Authenticates with GCP using a service account key to securely access resources.
2. **Data Extraction**: Queries Dataform workflow invocations and their associated assertion actions.
3. **Data Storage**: Writes the processed data into a **BigQuery table** for structured storage.
4. **View Creation**: Generates analytical views in BigQuery to summarize and visualize the assertion data.

## How It Works

### 1. **Authentication**
The script authenticates with GCP using a service account JSON key file. The `authenticate` function in `src/authentication.py` retrieves an OAuth token and credentials required to access GCP APIs.

### 2. **Data Extraction**
The `dataform_api.py` module connects to the Dataform API to fetch workflow invocations and their associated assertion actions. This data includes details such as:
- Start and end times
- Invocation and action names
- Assertion status (e.g., succeeded or failed)
- Failure reasons (if any)

### 3. **Data Storage**
The extracted data is loaded into a BigQuery table (`dataform_qaguard.assertion_data`) using the `load_to_bigquery` function in `bigquery_client.py`. The table schema includes:
- **Start_Time**, **End_Time**
- **Invocation_Name**, **Action_Name**
- **Database**, **Schema**
- **State** (e.g., SUCCEEDED, FAILED)
- **Failure_Reason**

### 4. **View Creation**
Two summary views are generated to provide insights:
- **Daily Recap View**:
  - Groups data by `assertion_date`
  - Shows total, passed, and failed assertions, along with failure percentages and duration metrics.
- **Action Summary View**:
  - Groups data by `Action_Name`
  - Summarizes the execution counts, success/failure rates, and duration metrics.

These views are created using SQL `CREATE OR REPLACE VIEW` statements in `bigquery_client.py`.

## Prerequisites

1. **Python 3.9+**: Install required dependencies from `requirements.txt`.
2. **Google Cloud Credentials**: A valid GCP service account with access to Dataform and BigQuery.
3. **BigQuery Dataset**: Ensure a dataset (`dataform_qaguard`) exists in your GCP project.

## Key Features

1. **Automated Quality Monitoring**: 
   - Fetches assertion results from Dataform workflows.
   - Automatically processes and loads data into BigQuery for analysis.

2. **Customizable Views**:
   - The views can be easily adapted to suit specific reporting needs by modifying the SQL templates in `bigquery_client.py`.

3. **Error Handling**:
   - Comprehensive logging for each operation ensures that issues such as missing data, API errors, or BigQuery failures are caught and reported.

4. **Scalable Architecture**:
   - Built using modular components to facilitate extensions or integration with other tools and workflows.

5. **CI/CD Integration**:
   - GitHub Actions workflow (`.github/workflows/run.yaml`) for seamless automation of the monitoring script.

## Contribution

Contributions are welcome! To contribute:

1. Fork the repository.
2. Create a new branch (`feature/my-feature`).
3. Commit your changes.
4. Open a pull request.

For major changes, please open an issue first to discuss the proposed update.

## Contact

For questions or support, please create an issue in this repository. Happy coding!