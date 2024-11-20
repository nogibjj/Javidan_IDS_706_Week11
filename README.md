[![CI](https://github.com/nogibjj/Javidan_IDS_706_Week11/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Javidan_IDS_706_Week11/actions/workflows/cicd.yml)

![alt text](https://miro.medium.com/v2/resize:fit:928/1*VMlX9-xnnS9Gc-vFgLwf0Q.png)

# Javidan_IDS_706_Week11

This repository contains materials and code for Week 11 of the IDS 706 course. This week focuses on creating a data ELT pipeline on Databricks and testing Databricks using REST APIs. The application utilizes a CSV file stored on GitHub, loads it into the Databricks Warehouse, and enhances data quality through transformations.

## Table of Contents
- [Overview](#overview)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Pipeline Structure][#pipeline]
- [Usage](#usage)


## Project Structure
The repository contains the following files and folders:
- `data/`: Sample datasets used in the assignments and projects.
- `mylib/`: Python scripts for data processing and analysis.

## Setup Instructions
To set up this project locally, follow these steps:

1. **Clone the repository**:
    ```bash
    git clone https://github.com/nogibjj/Javidan_IDS_706_Week11.git
    cd Javidan_IDS_706_Week11
    ```

2. **Install dependencies**:
    Make sure you have Python 3.8+ installed. Install required packages using:
    ```bash
    make install
    ```

## Pipeline
The pipeline consists of three distinct parts. The first step extracts the CSV file and loads it into memory. The second step processes the data and loads it into a predefined Databricks table. Finally, the third step reads this data, applies transformations, and stores the result in a different table.

![alt text](https://github.com/nogibjj/Javidan_IDS_706_Week11/blob/19a6551ae0705cd01a76032a19497beb901220fa/data/ELT_Pipeline.png)



## Usage
To start working with the notebooks and scripts, follow these guidelines:

- Open Databricks Workspace
- Click on Create button and choose the git
- Put the github repo under Git repository URL.

- Features

    ```bash
    python main.py
    ```

- To run tests:
    ```bash
    make tests
    ```

- To run formating:
    ```bash
    make format
    ```

- To run linting:
    ```bash
    make lint
    ```


## Related Images

![alt text](https://github.com/nogibjj/Javidan_IDS_706_Week11/blob/cc80d3a6462baafafa3fcf40ea6ba9b3be152c04/data/DatabricksTransformOutput.png)

![alt text](https://github.com/nogibjj/Javidan_IDS_706_Week11/blob/cc80d3a6462baafafa3fcf40ea6ba9b3be152c04/data/ClusterDetails.png)
