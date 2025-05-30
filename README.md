# Project Name
## Gohighlevel-Project

# Setup Instructions

## Installation

### Python Installation Process
Before proceeding, ensure Python is installed on your system. If not, you can download and install Python from [python.org](https://www.python.org/downloads/).

### Setting up a Virtual Environment
To work with Django, it's recommended to create a virtual environment. Follow the steps outlined in the [Python documentation](https://docs.python.org/3/tutorial/venv.html) or use tools like `virtualenv` or `venv`.

## Getting Started

### Clone the Project
```bash
git  https://github.com/exoticaitsolutions/Gohighlevel-merged-code.git
```

## Navigate to the Project Directory

```bash
  cd Gohighlevel-merged-code
```

# Install Dependencies
### Using requirements.txt
```
pip install -r requirements.txt
```

# Individual Dependencies

***Selenium***
```
pip install selenium
```

***Webdriver Manager***
```
pip install webdriver-manager
```
***db-dtypes***
```
pip install db-dtypes
```

## Setup .env File
Place the data-warehouse-437615-feb557543eaf.json file in the root directory of your project.
```
GOOGLE_CREDENTIALS = "data-warehouse-437615-feb557543eaf.json"
```

# Creating a Table for a New JSON in BigQuery
* If you want to create a table for a new JSON file, uncomment the create_table_workflow_actions() function in the bigquery_script.py file.
## Steps to Follow:
```
1. Open the bigquery_script.py file in your project.
2. Locate the crate_table_workflow_actions() and create_table_for_workflow_action_stats() function.
3. Uncomment the function call in the script.
4. Run the bigquery_script to create the table in BigQuery.
```

# Verifying Data Insertion in the Table
* To check if the data has been successfully inserted into the table, uncomment the show_data_in_workflow_actions() function in the bigquery_script.py file.
## Steps to Follow:
```
1. Open the bigquery_script.py file in your project.
2. Locate the show_data_in_workflow_action_stats() and show_data_for_actions_table() function.
3. Uncomment the function call in the script.
4. Run the bigquery_script to display the data from the table.
```

# Gernate token.json file
* If you do not have a token.json file for authenticating with the Gmail API, follow these steps to generate it:

1. Open generate_token_file.py: Open the Python file named generate_token_file.py.

2. Uncomment the Authentication Function: Locate the function authenticate_gmail_api() within the file. Uncomment this function if it is currently commented out. This function contains the logic for generating the necessary token file.
4. Run the File: Use the following command to run the script:
```
python gernate_token_file.py
```

# Run Project
```bash
python main.py
```

# Docker Setup
* Follow this documentation for setup in your system 
https://docs.google.com/document/d/1qwG3WvPFFUCOsp3oLpv5owO6k2PPBptTaa6kJLnZqlE/edit?tab=t.0

# Running the Application with Docker
## 1. Build the Docker Image
Run the following command to build the Docker image and tag it as python-app:
```
docker build -t python-app .
```

# Run the Docker Container
``
docker run -it --name my-python-container --shm-size=4g --memory=8g --cpus="4" python-app
``
#  if you want update json file outside the docker image  then run this command
```
docker run -it \
  --name my-python-container \
  --shm-size=4g \
  --memory=8g \
  --cpus="2" \
  -v /home/dell/Music/cred:/app/cred \  
  -e GOOGLE_CREDENTIALS="/app/cred/data-warehouse-437615-feb557543eaf.json" \
  -e CLIENT_SECRET_PATH="/app/cred/client_secret.json" \
  -e TOKEN_PATH="/app/cred/token.json" \
  python-app
```

# Stop the Container
To stop the running container, execute:
```
docker stop my-python-container
```

# Remove the Container
Once the container is stopped, you can remove it with:
```
docker rm my-python-container
```
