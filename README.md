# Child Care Data ETL Project

This project is an ETL (Extract, Transform, Load) service designed to process child care provider data from multiple sources and consolidate it into a single SQLite database.

## Installation and Setup

### Prerequisites

- Python 3.7 or higher
- pip (Python package installer)

### Installation Steps

1. Clone this repository:
   ```
   git clone https://github.com/your-username/child-care-data-etl.git
   cd child-care-data-etl
   ```

2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Running the Service

1. Ensure your Excel file is named "Technical Exercise Data.xlsx" and is in the project root directory.

2. Run the ETL script:
   ```
   python etl.py
   ```

3. The script will process the data and create a SQLite database named `child_care_data.db` in the project directory.

## Tradeoffs and Future Improvements

While developing this ETL service, several tradeoffs were made to balance functionality, time constraints, and simplicity:

1. **SQLite Database**: I used SQLite for simplicity, but for a production environment, a more robust database system like PostgreSQL would be more appropriate.

2. **Error Handling**: The current implementation has basic error handling. In a production system, more comprehensive error handling and logging would be crucial.

3. **Data Validation**: I implemented basic data cleaning and validation, like deduplication and standardizing formats of phone numbers and addresses. More rigorous validation checks could be added to ensure data integrity.

4. **Data Quality**: I perform basic data quality checks, but in the future I would have added in QA using Great Expectations to verify things like columns that are expected to be null, columns that are expected to be unique, verifying that certain columns values fall within an expected range, etc.

6. **Geocoding**: The current geocoding implementation is basic and may not handle all address formats. I use an external API that is free (TomTom Geocoding API) but the free level does have limits. A more sophisticated geocoding service could improve accuracyand not incur additional cost. *IMPORTANT NOTE*: I include the API key in this repository's .env file, to avoid the user having to go make an API account for testing this repo. THIS IS BAD PRACTICE, and should never be done. I created the key with a dummy email address and no identifying information. The key will be automatically destroyed 1 week from it's creation date.

7. **Performance**: For large datasets, the current row-by-row processing might be slow. Batch processing or using more efficient data processing libraries like pandas could improve performance.

8. **Testing**: The current version lacks unit tests. Implementing a comprehensive test suite would be a priority for a production-ready system.

Given additional time, these areas would be prime candidates for improvement.

## Long-term ETL Strategies

For a more scalable and robust ETL process in the long term, I would consider the following strategies:

1. **Schema Flexibility**: Implement a schema-on-read approach to handle varying file formats and changing schemas.

2. **Cloud-based Infrastructure**: Utilize cloud services (e.g., AWS S3, Azure Data Lake) for scalable storage and processing.

3. **Workflow Orchestration**: Use tools like Apache Airflow for scheduling and monitoring ETL jobs.

4. **Data Quality Framework**: Implement automated data quality checks using tools like Great Expectations. 


## Additional Notes

-Thank you for your time and consideration!

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
