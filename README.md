# Dynamic Name Cleaning Workflow

## Overview
This project features a dynamic **name cleaning workflow** developed using **Python**, **pandas**, and **MSSQL**. The primary goal is to standardize and sanitize name data effectively, ensuring consistency and accuracy across datasets.

## Features
- **Python & pandas**: Utilized for data processing and manipulation.
- **Regex-based Sanitization**: Applied regular expressions to clean and filter out unwanted characters from name data.
- **SQL-based Transformations**: Integrated with MSSQL for advanced data standardization using SQL functions.

## Workflow
1. **Data Ingestion**: Import raw name data using pandas.
2. **Data Cleaning**:
   - Apply regex for data sanitization (e.g., removing special characters, trimming whitespace).
3. **Database Integration**:
   - Load cleaned data into MSSQL.
   - Apply further SQL transformations for enhanced data standardization.
4. **Output**: Export the final standardized dataset for further use.

## Technologies Used
- **Python**: Core scripting language.
- **pandas**: Data manipulation and cleaning.
- **MSSQL**: Database for SQL-based transformations.
- **Regex**: For effective data sanitization.
