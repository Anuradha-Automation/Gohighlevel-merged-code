from datetime import datetime, timezone
import csv
from datetime import datetime
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from google.oauth2 import service_account
from dotenv import load_dotenv
from logging_setup import setup_logging


load_dotenv(dotenv_path='cred/.env')

# Access the google_credentials path from the .env file
google_credentials = os.getenv('GOOGLE_CREDENTIALS')
project_id = os.getenv('BIGQUERY_PROJECT_ID')
dataset_id = os.getenv('BIGQUERY_DATASET_ID')


# Load credentials from the service account file
credentials = service_account.Credentials.from_service_account_file(google_credentials)

# Initialize the BigQuery client with credentials
client = bigquery.Client(credentials=credentials, project=project_id)

logger = setup_logging()


def show_all_table_in_database():
    # Reference the dataset
    dataset_ref = client.dataset(dataset_id)

    # List all tables in the dataset
    tables = client.list_tables(dataset_ref)  # API call
    for table in tables:
        logger.info(table.table_id)

def crate_table_workflow_actions():
    table_id = "workflow_actions"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED", description="Unique ID for each row"),
        bigquery.SchemaField("workflow_id", "STRING", mode="REQUIRED", description="ID of the workflow from the workflow table"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED", description="Name of the category"),
        bigquery.SchemaField("step", "INTEGER", mode="REQUIRED", description="Category_id of parent step number in workflow"),
        bigquery.SchemaField("type", "STRING", mode="REQUIRED", description="Email, SMS, etc."),
        bigquery.SchemaField("last_updated_date", "TIMESTAMP", mode="REQUIRED", description="Last updated timestamp"),
    ]

    # Check if the table already exists
    try:
        client.get_table(table_ref)  # API call to fetch table details
        logger.info(f"Table '{table_id}' already exists in dataset '{dataset_id}'.")
    except Exception as e:
        if "Not found" in str(e):
            # Create the table if it doesn't exist
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)  # API call
            logger.info(f"Table '{table_id}' created successfully in dataset '{dataset_id}'.")
        else:
            logger.info(f"Table '{table_id}' already created in dataset '{dataset_id}'.")


def create_table_for_workflow_action_stats():
    table_id = "workflow_actions_stats"

    # Load credentials from the service account file
    credentials = service_account.Credentials.from_service_account_file(google_credentials)

    # Initialize the BigQuery client with credentials
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Step 1: Define the table schema
    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED", description="Unique ID for each row"),
        bigquery.SchemaField("workflow_action_id", "STRING", mode="REQUIRED", description="ID of the workflow action from the workflow action table"),
        bigquery.SchemaField("last_updated_date", "TIMESTAMP", mode="REQUIRED", description="Last updated timestamp"),
        
        bigquery.SchemaField("stats_sms_count_total", "INTEGER", mode="REQUIRED", description="Total SMS count"),
        bigquery.SchemaField("stats_sms_count_delivered", "INTEGER", mode="REQUIRED", description="Delivered SMS count"),
        bigquery.SchemaField("stats_sms_count_clicked", "INTEGER", mode="REQUIRED", description="Clicked SMS count"),
        bigquery.SchemaField("stats_sms_count_failed", "INTEGER", mode="REQUIRED", description="Failed SMS count"),
        bigquery.SchemaField("stats_sms_percent_delivered", "STRING", mode="REQUIRED", description="Delivered percentage of SMS"),
        bigquery.SchemaField("stats_sms_percent_clicked", "STRING", mode="REQUIRED", description="Clicked percentage of SMS"),
        bigquery.SchemaField("stats_sms_percent_failed", "STRING", mode="REQUIRED", description="Failed percentage of SMS"),

        bigquery.SchemaField("stats_email_count_total", "INTEGER", mode="REQUIRED", description="Total email count"),
        bigquery.SchemaField("stats_email_count_delivered", "INTEGER", mode="REQUIRED", description="Delivered email count"),
        bigquery.SchemaField("stats_email_count_opened", "INTEGER", mode="REQUIRED", description="Opened email count"),
        bigquery.SchemaField("stats_email_count_clicked", "INTEGER", mode="REQUIRED", description="Clicked email count"),
        bigquery.SchemaField("stats_email_count_replied", "INTEGER", mode="REQUIRED", description="Replied email count"),
        bigquery.SchemaField("stats_email_count_bounced", "INTEGER", mode="REQUIRED", description="Bounced email count"),
        bigquery.SchemaField("stats_email_count_unsubscribed", "INTEGER", mode="REQUIRED", description="Unsubscribed email count"),
        bigquery.SchemaField("stats_email_count_rejected", "INTEGER", mode="REQUIRED", description="Rejected email count"),
        bigquery.SchemaField("stats_email_count_complained", "INTEGER", mode="REQUIRED", description="Complained email count"),

        bigquery.SchemaField("stats_email_percent_delivered", "STRING", mode="REQUIRED", description="Delivered percentage of emails"),
        bigquery.SchemaField("stats_email_percent_opened", "STRING", mode="REQUIRED", description="Opened percentage of emails"),
        bigquery.SchemaField("stats_email_percent_clicked", "STRING", mode="REQUIRED", description="Clicked percentage of emails"),
        bigquery.SchemaField("stats_email_percent_replied", "STRING", mode="REQUIRED", description="Replied percentage of emails"),
        bigquery.SchemaField("stats_email_percent_bounced", "STRING", mode="REQUIRED", description="Bounced percentage of emails"),
        bigquery.SchemaField("stats_email_percent_unsubscribed", "STRING", mode="REQUIRED", description="Unsubscribed percentage of emails"),
        bigquery.SchemaField("stats_email_percent_rejected", "STRING", mode="REQUIRED", description="Rejected percentage of emails"),
        bigquery.SchemaField("stats_email_percent_complained", "STRING", mode="REQUIRED", description="Complained percentage of emails"),
    ]

    # Define the table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Check if the table already exists
    try:
        client.get_table(table_ref)
        logger.info(f"Table '{table_id}' already exists in dataset '{dataset_id}'.")
    except Exception as e:
        if "Not found" in str(e):
            # Create the table if it doesn't exist
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)  # API call
            logger.info(f"Table '{table_id}' created successfully in dataset '{dataset_id}'.")
        else:
            raise  

        
# today_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
today_date = datetime.utcnow().strftime('%Y-%m-%d')


def show_data_for_actions_table():
    table_id = "workflow_actions"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Query to fetch all records
    total_query = f"""
    SELECT COUNT(*) as total_records
    FROM `{table_ref}`
    """
    total_count_job = client.query(total_query)
    total_records = total_count_job.result().to_dataframe().iloc[0]['total_records']

    # Query to fetch records updated today
    today_query = f"""
    SELECT COUNT(*) as today_records
    FROM `{table_ref}`
    WHERE DATE(last_updated_date) = '{today_date}'
    """
    today_count_job = client.query(today_query)
    today_records = today_count_job.result().to_dataframe().iloc[0]['today_records']

    logger.info(f" Workflow Actions : {today_records} / {total_records}")
    return f"{today_records} / {total_records}"


def show_data_in_workflow_action_stats():
    table_id = "workflow_actions_stats"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Query to get the total count of records
    total_count_query = f"""
    SELECT COUNT(*) as total_records
    FROM `{table_ref}`
    """

    # Query to get the count of records updated or inserted today
    today_count_query = f"""
    SELECT COUNT(*) as today_records
    FROM `{table_ref}`
    WHERE DATE(last_updated_date) = '{today_date}'
    """

    # Run total count query
    total_count_job = client.query(total_count_query)
    total_records = total_count_job.result().to_dataframe().iloc[0]['total_records']

    # Run today's count query
    today_count_job = client.query(today_count_query)
    today_records = today_count_job.result().to_dataframe().iloc[0]['today_records']

    logger.info(f" Workflow Action Stats : {today_records} / {total_records}")
    return f"{today_records} / {total_records}"

def delete_table_workflow_actions():
    table_id = "workflow_actions"

    # Define the table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Load credentials from the service account file
    credentials = service_account.Credentials.from_service_account_file(google_credentials)

    # Initialize the BigQuery client with credentials
    client = bigquery.Client(credentials=credentials, project=project_id)

    try:
        # Attempt to delete the table
        client.delete_table(table_ref)
        logger.info(f"Table '{table_id}' deleted successfully from dataset '{dataset_id}'.")
    except Exception as e:
        if "Not found" in str(e):
            logger.warning(f"Table '{table_id}' does not exist in dataset '{dataset_id}'.")
        else:
            logger.error(f"An error occurred while deleting the table '{table_id}': {str(e)}")

def delete_table_for_workflow_action_stats():
    table_id = "workflow_actions_stats"

    # Load credentials from the service account file
    credentials = service_account.Credentials.from_service_account_file(google_credentials)

    # Initialize the BigQuery client with credentials
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Define the table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        # Attempt to delete the table
        client.delete_table(table_ref)
        logger.info(f"Table '{table_id}' deleted successfully from dataset '{dataset_id}'.")
    except Exception as e:
        if "Not found" in str(e):
            logger.warning(f"Table '{table_id}' does not exist in dataset '{dataset_id}'.")
        else:
            logger.error(f"An error occurred while deleting the table '{table_id}': {str(e)}")

# # Create tables uncomment this 
# create_table_for_workflow_action_stats()
# crate_table_workflow_actions()

# Delete tables uncomment this  
# delete_table_workflow_actions()
# delete_table_for_workflow_action_stats()

# Show Data In Database
# show_data_in_workflow_action_stats()
# show_data_for_actions_table()