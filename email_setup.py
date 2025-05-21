import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv

from big_query_script import show_data_for_actions_table, show_data_in_workflow_action_stats
from insert_data_bigquery import count_functionality
from logging_setup import setup_logging


load_dotenv(dotenv_path='cred/.env')

logger = setup_logging()

# Access the environment variables
username = os.getenv('EMAIL') 
password = os.getenv('PASSWORD')  
to_email = os.getenv('RECEIVER_EMAILS')  # Comma-separated emails
smtp_server = os.getenv('SMPT_HOST')  
port = os.getenv('SMPT_PORT')  
from_email = os.getenv('FROM_EMAIL')  
stats_email_subject = os.getenv('STATS_EMAIL_SUBJECT')  
token_email_subject = os.getenv('TOKEN_EMAIL_SUBJECT')  

def send_email(start_time, total_time):
    workflow_actions_stats_success_count, WORKFLOW_ACTIONS_STATS_COUNT, workflow_actions_success_count, WORKFLOW_ACTIONS_COUNT = count_functionality()

    text_body= f"""
        Start Time : {start_time}
        Workflow Actions: {workflow_actions_success_count}/{WORKFLOW_ACTIONS_COUNT}
        Workflow Action Stats:   {workflow_actions_stats_success_count}/{WORKFLOW_ACTIONS_STATS_COUNT}
        Total Processing Time : {total_time}
        """
    print("text_body : ", text_body)
    try:
        # Create the email
        message = MIMEMultipart()
        message['From'] = from_email
        message['Subject'] = stats_email_subject
        
        # Split `to_email` into a list of email addresses
        recipient_list = [email.strip() for email in to_email.split(',')]
        
        # Join all recipients into the `To` header
        message['To'] = ", ".join(recipient_list)
        message.attach(MIMEText(text_body, 'plain'))
        
        # Connect to the SMTP server
        with smtplib.SMTP(smtp_server, int(port)) as server:
            server.starttls()  # Upgrade connection to secure
            server.login(username, password)  # Log in with SMTP credentials
            # Send the email
            server.sendmail(from_email, recipient_list, message.as_string())
            logger.info("Email sent successfully!")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        
# send_email('2025-01-15 10:09:23.645788',  '0:00:00')


def send_email_for_token_expire():
    text_body= 'Your token has expired. Please reauthenticate to continue.'
    try:
        # Create the email
        message = MIMEMultipart()
        message['From'] = from_email
        message['Subject'] = token_email_subject
        
        # Split `to_email` into a list of email addresses
        recipient_list = [email.strip() for email in to_email.split(',')]
        
        # Join all recipients into the `To` header
        message['To'] = ", ".join(recipient_list)
        message.attach(MIMEText(text_body, 'plain'))
        
        # Connect to the SMTP server
        with smtplib.SMTP(smtp_server, int(port)) as server:
            server.starttls()  # Upgrade connection to secure
            server.login(username, password)  # Log in with SMTP credentials
            # Send the email
            server.sendmail(from_email, recipient_list, message.as_string())
            logger.info("Email sent successfully!")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")

# send_email_for_token_expire()