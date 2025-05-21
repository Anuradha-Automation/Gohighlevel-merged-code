from datetime import datetime
from big_query_script import client
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from logging_setup import setup_logging

WORKFLOW_ACTIONS_COUNT = 0
WORKFLOW_ACTIONS_EXCEPTION = 0

WORKFLOW_ACTIONS_STATS_COUNT = 0
WORKFLOW_ACTIONS_STATS_EXCEPTION = 0

# Load environment variables from the .env file
load_dotenv(dotenv_path='cred/.env')

project_id = os.getenv('BIGQUERY_PROJECT_ID')
dataset_id = os.getenv('BIGQUERY_DATASET_ID')

logger = setup_logging()

def get_last_workflow_id():
    table_id = "workflow_actions"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    query = f"""
        SELECT MAX(id) AS last_workflow_id
        FROM `{table_ref}`
    """
    try:
        query_job = client.query(query)  # Execute the query
        result = list(query_job)  # Convert RowIterator to a list
        
        # Extract the value from the result
        if result:  # Check if result has any rows
            last_id = result[0].last_workflow_id or 0  # Access the 'last_workflow_id' field
        else:
            last_id = 0  # No rows, default to 0
        
        return last_id

    except Exception as e:
        return 0
    
def get_current_utc_time():
    # Get the current system time in UTC
    current_utc_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    return current_utc_time

# # Example usage
LastUpdatedTime = get_current_utc_time()


def insert_data_in_work_flow_actions(rows_to_insert):
    global WORKFLOW_ACTIONS_EXCEPTION
    global WORKFLOW_ACTIONS_COUNT
    table_id = "workflow_actions"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        WORKFLOW_ACTIONS_COUNT += 1
        row = rows_to_insert[0]  # Assuming only one row to process at a time
        workflow_id = row["workflow_id"]
        name = row["name"]
        step = row["step"]
        type = row["type"]

        # Query to check if the combination of workflow_id, name, and step exists
        query = f"""
        SELECT id 
        FROM `{table_ref}` 
        WHERE workflow_id = @workflow_id AND step = @step
        """
        # Use query parameters to avoid string literal issues
        query_job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("workflow_id", "STRING", workflow_id),
                    bigquery.ScalarQueryParameter("step", "INT64", step),
                ]
            ),
        )            
        results = list(query_job.result())

        if results:
            row_id = results[0].id
            logger.info(f"Row with id {row_id} data already exists.")
            
            query = f"""
                UPDATE `{table_ref}`
                SET 
                    name = @name,
                    type = @type,
                    last_updated_date = @last_updated_date
                WHERE id = @row_id
            """
            
            query_job = client.query(
            query=query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("name", "STRING", name),
                    bigquery.ScalarQueryParameter("type", "STRING", type),  # Add the type parameter
                    bigquery.ScalarQueryParameter("last_updated_date", "TIMESTAMP", LastUpdatedTime),
                    bigquery.ScalarQueryParameter("row_id", "INT64", row_id),
                ]
            ),
        )
            query_job.result()  # Wait for query to complete
            logger.info(f"Row with ID {row_id} was updated successfully.")
            return row_id
    
        else:
            # If no matching row exists, insert the new data
            last_workflow_id = get_last_workflow_id()
            new_workflow_id = 1 if last_workflow_id == 0 else last_workflow_id + 1
            row["id"] = new_workflow_id

            # Ensure the date format is correct
            last_updated_date = row.get("last_updated_date", datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'))
            row["last_updated_date"] = last_updated_date

            errors = client.insert_rows_json(table_ref, [row])  # Insert the row
            if not errors:
                logger.info("Data inserted successfully into workflow_actions table.")
                return new_workflow_id
            else:   
                return {"success": False, "error": errors}
    except Exception as e:
        WORKFLOW_ACTIONS_EXCEPTION += 1
        exception_message = {"success": False, "error": str(e)}
        return exception_message


def insert_data_into_workflow_actions_stats(workflow_action_id, email_stats_data, sms_stats_data):
    global WORKFLOW_ACTIONS_STATS_COUNT
    global WORKFLOW_ACTIONS_STATS_EXCEPTION
    table_id = "workflow_actions_stats"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        WORKFLOW_ACTIONS_STATS_COUNT = WORKFLOW_ACTIONS_STATS_COUNT+1
        # Fetch the current maximum workflow_action_id
        query = f"""
            SELECT MAX(id) AS max_id
            FROM `{table_ref}`
        """
        query_job = client.query(query)  # Execute the query
        max_id = None
        for row in query_job:
            max_id = row["max_id"]

        next_id = (max_id + 1) if max_id is not None else 1  # Start from 1 if no records exist

         # Convert percentages to floats for calculation
        stats_email_count_total = int(email_stats_data.get("stats_email_total_count", 0))

        stats_sms_count_total = int(sms_stats_data.get("stats_sms_total_count", 0))


        stats_email_percent_delivered = float(email_stats_data.get("stats_email_percent_delivered", 0))
        stats_sms_percent_delivered = float(sms_stats_data.get("stats_sms_percent_delivered", 0))
        stats_sms_percent_clicked = float(sms_stats_data.get("stats_sms_percent_clicked", 0))
        stats_sms_percent_failed = float(sms_stats_data.get("stats_sms_percent_failed", 0))

        stats_email_count_delivered = round(stats_email_count_total * stats_email_percent_delivered / 100)
        stats_email_count_opened = round(stats_email_count_total * float(email_stats_data.get("stats_email_percent_opened", 0)) / 100)
        stats_email_count_clicked = round(stats_email_count_total * float(email_stats_data.get("stats_email_percent_clicked", 0)) / 100)
        stats_email_count_replied = round(stats_email_count_total * float(email_stats_data.get("stats_email_percent_replied", 0)) / 100)
        stats_email_count_bounced = round(stats_email_count_total * float(email_stats_data.get("stats_email_percent_bounced", 0)) / 100)
        stats_email_count_unsubscribed = round(stats_email_count_total * float(email_stats_data.get("stats_email_percent_unsubscribed", 0)) / 100)
        stats_email_count_rejected = round(stats_email_count_total * float(email_stats_data.get("stats_email_percent_rejected", 0)) / 100)
        stats_email_count_complained = round(stats_email_count_total * float(email_stats_data.get("stats_email_percent_complained", 0)) / 100)

        stats_sms_count_delivered = round(stats_sms_count_total * stats_sms_percent_delivered / 100)
        stats_sms_count_clicked = round(stats_sms_count_total * stats_sms_percent_clicked / 100)
        stats_sms_count_failed = round(stats_sms_count_total * stats_sms_percent_failed / 100)

        # Prepare the data to insert based on the table schema
        rows_to_insert = [
            {
                "id": next_id,
                "workflow_action_id": workflow_action_id,
                "last_updated_date": email_stats_data.get("last_updated_date", get_current_utc_time()),
                
                # SMS statistics
                "stats_sms_count_total": round(stats_sms_count_total),
                "stats_sms_count_delivered": round(stats_sms_count_delivered),
                "stats_sms_count_clicked": round(stats_sms_count_clicked),
                "stats_sms_count_failed": round(stats_sms_count_failed),

                "stats_sms_percent_delivered": stats_sms_percent_delivered,
                "stats_sms_percent_clicked": stats_sms_percent_clicked,
                "stats_sms_percent_failed": stats_sms_percent_failed,
                
                # Email statistics
                "stats_email_count_total": round(stats_email_count_total),
                "stats_email_count_delivered": round(stats_email_count_delivered),
                "stats_email_count_opened": round(stats_email_count_opened),
                "stats_email_count_clicked": round(stats_email_count_clicked),
                "stats_email_count_replied": round(stats_email_count_replied),
                "stats_email_count_bounced": round(stats_email_count_bounced),
                "stats_email_count_unsubscribed": round(stats_email_count_unsubscribed),
                "stats_email_count_rejected": round(stats_email_count_rejected),
                "stats_email_count_complained": round(stats_email_count_complained),

    
                "stats_email_percent_delivered": stats_email_percent_delivered,
                "stats_email_percent_opened": email_stats_data.get("stats_email_percent_opened", 0),
                "stats_email_percent_clicked": email_stats_data.get("stats_email_percent_clicked", 0),
                "stats_email_percent_replied": email_stats_data.get("stats_email_percent_replied", 0),
                "stats_email_percent_bounced": email_stats_data.get("stats_email_percent_bounced", 0),
                "stats_email_percent_unsubscribed": email_stats_data.get("stats_email_percent_unsubscribed", 0),
                "stats_email_percent_rejected": email_stats_data.get("stats_email_percent_rejected", 0),
                "stats_email_percent_complained": email_stats_data.get("stats_email_percent_complained", 0),
            }
        ]

        # Insert rows into the table
        errors = client.insert_rows_json(table_ref, rows_to_insert)  # API call
        if not errors:
            success_message = {"success": True, "message": "Data inserted successfully"}
            logger.info("Data inserted successfully into workflow_actions_stats table!")
            return success_message  # Success case
        else:
            error_message = {"success": False, "error": errors}
            logger.error(f"Data insertion failed: {error_message}")
            return error_message  # Error case
        
    except Exception as e:
        WORKFLOW_ACTIONS_STATS_EXCEPTION=WORKFLOW_ACTIONS_STATS_EXCEPTION+1
        exception_message = {"success": False, "error": str(e)}
        logger.error(f"Exception occurred during data insertion: {exception_message}")
        return exception_message  # Exception case
    
def count_functionality():
    global WORKFLOW_ACTIONS_STATS_COUNT
    global WORKFLOW_ACTIONS_STATS_EXCEPTION 
    global WORKFLOW_ACTIONS_EXCEPTION
    global WORKFLOW_ACTIONS_COUNT
    workflow_actions_stats_success_count = WORKFLOW_ACTIONS_STATS_COUNT - WORKFLOW_ACTIONS_STATS_EXCEPTION
    workflow_actions_success_count = WORKFLOW_ACTIONS_COUNT - WORKFLOW_ACTIONS_EXCEPTION
    return workflow_actions_stats_success_count, WORKFLOW_ACTIONS_STATS_COUNT, workflow_actions_success_count, WORKFLOW_ACTIONS_COUNT
