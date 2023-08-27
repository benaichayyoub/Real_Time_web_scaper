# scraper.py
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from kafka import KafkaProducer

# Initialize Selenium WebDriver
driver = webdriver.Chrome(executable_path='path_to_chromedriver')
driver.get("https://www.indeed.com")

# Search for data engineering job listings
search_box = driver.find_element_by_id("text-input-what")
search_box.send_keys("Data Engineer")
search_box.send_keys(Keys.RETURN)

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

while True:
    job_listings = driver.find_elements_by_class_name("jobsearch-SerpJobCard")

    for job in job_listings:
        job_title = job.find_element_by_class_name("title").text
        company_name = job.find_element_by_class_name("company").text
        location = job.find_element_by_class_name("location").text
        
        data = {
            "job_title": job_title,
            "company_name": company_name,
            "location": location
        }
        producer.send("data_engineer_job_topic", value=data)
    
    next_button = driver.find_element_by_class_name("pagination-next")
    next_button.click()

# Close connections
producer.close()
driver.quit()
