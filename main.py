from datetime import datetime
import time
# from get_otp import login_gmail
from email_setup import send_email
from login_with_google_api import otp_get_from
from urls import *
from utils import *
from logging_setup import setup_logging
from webdriver_configration import driver_confrigration
from selenium.webdriver.common.by import By
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv(dotenv_path='cred/.env')

# Access the google_credentials path from the .env file
GOHIGHLEVEL_EMAIL = os.getenv('GOHIGHLEVEL_EMAIL')
GOHIGHLEVEL_PASSWORD = os.getenv('GOHIGHLEVEL_PASSWORD')


logger = setup_logging()

def scrapping():
    logger.info("Starting the scrapping process.")
    
    # Record the start time
    start_time = datetime.now()
    logger.info(f"Script started at: {start_time}")
    
    try:
        driver = driver_confrigration()
        logger.info("Driver configuration completed.")
        
        driver.get(WEBSITE_URL)
        logger.info(f"Navigated to {WEBSITE_URL}.")
        time.sleep(20)
        
        email_box = driver.find_element(By.ID, 'email')
        email_box.send_keys(GOHIGHLEVEL_EMAIL)
        logger.info(f"Entered email {GOHIGHLEVEL_EMAIL}.")
        
        password_box = driver.find_element(By.ID, 'password')
        password_box.send_keys(GOHIGHLEVEL_PASSWORD)
        logger.info("Entered password.")
        
        signup_button = driver.find_element(By.XPATH, '//*[@id="app"]/div[1]/div[4]/section/div[2]/div/div/div/div[4]/div/button')
        signup_button.click()
        logger.info("Clicked on signup button.")
        
        time.sleep(5)
        
        send_security_code = driver.find_element(By.XPATH, '//*[@id="app"]/div[1]/div[4]/section/div[2]/div/div/div/div[3]/div/button')
        send_security_code.click()
        logger.info("Sent security code.")
        
        time.sleep(50)
        logger.info("Waiting for OTP.")
        
        # Uncomment and integrate OTP processing as needed
        security_code = otp_get_from()
        time.sleep(10)
        logger.info(f"OTP fetched successfully: {security_code}")
        otp_digits = list(str(security_code))
        otp_inputs = driver.find_elements(By.CLASS_NAME, 'otp-input')
        for i, digit in enumerate(otp_digits):
            otp_inputs[i].send_keys(digit)
        time.sleep(60)
        
        logger.info("Login successful.")
        
        automation_button = driver.find_element(By.ID, "sb_automation")
        automation_button.click()
        logger.info("Navigated to automation.")
        time.sleep(60)
        
        try:
            driver.switch_to.frame("workflow-builder")
            time.sleep(6)
            logger.info("Switched to iframe 'workflow-builder'.")
        except Exception as e:
            logger.error(f"Iframe not found: {e}")
        
        time.sleep(10)
        
        main_publish_list = status_check_folder_or_not(driver)
        logger.info(f"Publish list obtained: {main_publish_list}")
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Scrapping completed at: {end_time}")
        logger.info(f"Total time taken for scrapping: {duration}")
        send_email(start_time, duration)
    
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
    
    logger.info("Scrapping process completed.")

# Call the function
scrapping()




