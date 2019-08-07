import pandas as pd
from selenium import webdriver
from time import sleep
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver import ActionChains
import numpy as np
import math
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options

d = '/Users/ritvikverma/Desktop'
options = webdriver.ChromeOptions()
prefs = {'download.default_directory' : d}
options.add_experimental_option('prefs', prefs)
driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
driver.maximize_window()



def get_industry_list():
    df = pd.read_csv('./spx_companymapping - constituents_new.csv')
    df = df[df['status'] == 'done']
    list_industries = list(set(df['sub_industry']))
    return(list_industries)

def navigate_to_element(path_to_industry):
    right_arrow = driver.find_element_by_class_name("docs-sheet-right")
    left_arrow = driver.find_element_by_class_name("docs-sheet-left")
    element = driver.find_element_by_xpath(path_to_industry)
    while (element.text == '' and 'docs-sheet-right-disabled' not in right_arrow.get_attribute("class")):
        right_arrow.click()
        element = driver.find_element_by_xpath(path_to_industry)

    while (element.text == '' and 'docs-sheet-left-disabled' not in left_arrow.get_attribute("class")):
        left_arrow.click()
        element = driver.find_element_by_xpath(path_to_industry)
    return element



def start_crawling():
    list_industries = get_industry_list()
    driver.get('https://docs.google.com/spreadsheets/d/1rsSY_s-fYsW764w0EegGeHSfZ4c2PeCwtuXIjHsrXHA/edit#gid=1297106814')
    time.sleep(5)
    arrow = driver.find_element_by_class_name("docs-sheet-right")
    for industry_name in list_industries:
        path_to_industry = "//*[contains(text(),\'"+industry_name + "\') and @class= 'docs-sheet-tab-name']"
        element= driver.find_element_by_xpath(path_to_industry)
        if(element.text==''):
            element = navigate_to_element(path_to_industry)
        sleep(1)
        element.click()
        curr_url = driver.current_url
        curr_url = curr_url.replace('/edit#gid=', '/export?format=csv&gid=')
        driver.get(curr_url)
        print('Clicked and downloaded', element.text)

if __name__ == "__main__":
    start_crawling()