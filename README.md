# Eterna Analysis Project

## Overview

This project contains a set of PySpark-based data analysis functions for processing and analyzing sales and employee data. It includes various analyses such as IT data extraction, marketing address information, department breakdowns, top performers, and more.

## Table of Contents

1. [Requirements](#requirements)
2. [Installation](#installation)
3. [Project Structure](#project-structure)
4. [Usage](#usage)
5. [Running Tests](#running-tests)
6. [Functions Overview](#functions-overview)
7. [Logging](#logging)
8. [Contributing](#contributing)
9. [License](#license)

## Requirements

- Python 3.10
- PySpark 2.51+
- chispa (for testing)
- pytest
- ruff
- build
- mypy
- pre-commit
## Installation

1. Clone the repository:
   ```
   git clone https://github.com/wasimaj4/Eternal-TeleSales-analysis.git
   cd Eternal-TeleSales-analysis
   ```

2. Create a virtual environment (optional but recommended):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Project Structure

```
eterna-analysis/
│
├── src/
│   ├── analysis/
│   │   ├── it_data.py
│   │   ├── marketing_address_info.py
│   │   ├── department_breakdown.py
│   │   ├── top_3.py
│   │   ├── top_3_most_sold_per_department_nl.py
│   │   ├── best_salesperson.py
│   │   ├── extra_insight_one.py
│   │   └── extra_insight_two.py
│   │
│   └── utils/
│       ├── log_manager.py
│       └── writer_csv.py
│
├── tests/
│   └── test_analysis.py
│
├── data/
│   ├── dataset_one.csv
│   ├── dataset_two.csv
│   └── dataset_three.csv
│
├── requirements.txt
└── README.md
```

## Usage

1. Ensure you're in the project root directory and your virtual environment is activated.

2. To run the main analysis:

   ```python
   from src.analysis.main import analysis

   file1 = "path/to/dataset_one.csv"
   file2 = "path/to/dataset_two.csv"
   file3 = "path/to/dataset_three.csv"

   analysis(file1, file2, file3)
   ```

   This will perform all analyses and save the results as CSV files in the current directory.

3. To use individual analysis functions:

   ```python
   from pyspark.sql import SparkSession
   from src.analysis.it_data import it_data

   spark = SparkSession.builder.appName("EternaAnalysis").getOrCreate()
   df_work_info = spark.read.csv("path/to/work_info.csv", header=True)
   df_personal = spark.read.csv("path/to/personal_info.csv", header=True)

   result = it_data(df_work_info, df_personal)
   result.show()
   ```

## Running Tests

To run the tests:

```
python -m pytest tests/test_analysis.py
```

## Functions Overview

1. `it_data`: Extracts IT department data.
2. `marketing_address_info`: Processes marketing department address information.
3. `department_breakdown`: Provides a breakdown of departments' performance.
4. `top_3`: Identifies top 3 performers in each department.
5. `top_3_most_sold_per_department_netherlands`: Analyzes top 3 most sold products per department in the Netherlands.
6. `best_salesperson`: Determines the best salesperson.
7. `extra_insight_one`: Provides additional insight 
8. `extra_insight_two`: Provides another additional insight 

## Logging

The project uses Python's built-in logging module. Logs are configured in `src/utils/log_manager.py`. By default, logs are written to both console and a file named `utils.logs`.



