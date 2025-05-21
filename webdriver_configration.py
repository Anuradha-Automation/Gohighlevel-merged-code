from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
import traceback

# def driver_confrigration():
#     options = webdriver.ChromeOptions()
#     options.add_argument("--disable-notifications")

#     options.add_argument("--start-maximized")

#     # options.add_argument("--headless")
    
#     # Use Service for ChromeDriverManager
#     service = Service(ChromeDriverManager().install())
    
#     # Pass options and service to Chrome WebDriver
#     driver = webdriver.Chrome(service=service, options=options)

#     return driver

# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from seleniumbase import Driver


# def driver_confrigration():
#     options = Options()
#     options.add_argument("--disable-notifications")
#     options.add_argument("--start-maximized")
#     driver = Driver(uc=True, headless=False)
#     # driver = webdriver.Chrome(service=Service(), options=options)
#     return driver


# FOR DOCKER------------------------------------------------------------


def driver_confrigration():
    try:
        print("driver function called")
        
        # Set Chrome options
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument('--disable-gpu')
        options.add_argument("--remote-debugging-port=9222") 

        # Setup Chrome driver service
        service = Service(executable_path="chromedriver-linux64/chromedriver")

        # Start driver
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(180)
        driver.set_script_timeout(180)

        print("driver ran successfully")
        return driver

    except Exception as e:
        print("Error while initializing the driver:")
        print(f"Exception: {e}")
        traceback.print_exc()
        return None
