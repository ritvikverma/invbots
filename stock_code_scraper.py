from selenium import webdriver
from time import sleep
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as bs
import pandas as pd
from selenium.webdriver.support import expected_conditions as EC
import os
driver = webdriver.Chrome(ChromeDriverManager().install())
url = 'https://www.hkexnews.hk/stocklist_active_main.htm'
driver.get(url)
driver.maximize_window()
wait = WebDriverWait(driver, 5)
wait_more = WebDriverWait(driver, 5)
header = ['Release Time', 'Stock Code', 'Stock Short Name', 'Document Headline', 'Document Link']
sleep(10)
soup = bs(driver.page_source, features="html.parser")
stock_code = []
for a in soup.find_all('td'):
    if(a.text.isdigit()):
        stock_code.append(a.text)


stock_url = 'https://www1.hkexnews.hk/search/titlesearch.xhtml'
driver.get(stock_url)
sleep(2)

start_index = 9999999999


try:
    data = pd.read_csv('~/Desktop/all_stocks.csv')
    print("FOUND")
    print('Length of DataFrame=', len(data))
    start_index = data.tail(1)['Stock Code'].values[0]
    print(start_index)
    start = stock_code.index(f"{int(start_index):05d}")
except:
    print("NOT FOUND")
    start = 0





print('Starting at', f"{int(start_index):05d}")

for code in stock_code[start:]:
    all_rows = []
    unscraped_codes = []
    if int(code)>10000:
        break
    e = driver.find_element_by_id('searchStockCode')
    e.clear()
    e.send_keys(code)
    try:
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "keyword-highlight")))
    except:
        unscraped_codes.append([code])
        try:
            old_unscraped = pd.read_csv('~/Desktop/unscraped_codes.csv')
            unscraped_codes_df = pd.DataFrame(unscraped_codes)
            old_unscraped.append(unscraped_codes_df)
            old_unscraped.to_csv('~/Desktop/unscraped_codes.csv', mode='a', index=True)
        except:
            unscraped_codes_df = pd.DataFrame(unscraped_codes)
            unscraped_codes_df.to_csv('~/Desktop/unscraped_codes.csv', index=True)
        continue

    suggestion = driver.find_element_by_class_name('keyword-highlight')
    driver.execute_script("arguments[0].click();", suggestion)
    search_path = "//*[contains(text(), \'SEARCH\')]"
    search = driver.find_element_by_xpath(search_path)
    driver.execute_script("arguments[0].click();", search)
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "visibility_visible")))
    while (True):
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            load_more = driver.find_element_by_class_name('component-loadmore__icon')
            # driver.execute_script("arguments[0].click();", load_more)
            load_more.click()
        except Exception as e:
            break

    soup = bs(driver.page_source, features="html.parser")
    release_times = [a.text.replace('Release Time:', '').strip() for a in soup.find_all("td", {"class": "release-time"})]
    try:
        total_records = driver.find_element_by_class_name("total-records").text
        hit_or_miss = int((str(len(release_times))+'/'+ str(total_records.replace('Total records found: ', ''))))
        print('For '+ str(code) + ', ' + str(hit_or_miss))
    except:
        print('For', code, 'ERROR')
    stock_short_names = [a.text.replace('Stock Short Name:', '').strip() for a in soup.find_all("td", {"class": "stock-short-name"})]
    headlines = [a.text.strip() for a in soup.find_all("div", {"class": "headline"})]
    links = [('https://www1.hkexnews.hk' + m.a['href']) for m in soup.find_all("div", {"class": "doc-link"})]
    for i, time in enumerate(release_times):
        all_rows.append([time, code, stock_short_names[i], headlines[i], links[i]])
    df = pd.DataFrame(all_rows, columns=header)

    try:  # else it exists so append without writing the header
        df.to_csv('~/Desktop/all_stocks.csv', mode='a', index=False)
        print('APPENDED')
    except:
        df.to_csv('~/Desktop/all_stocks.csv', index=False)
        print('REWRITTEN')