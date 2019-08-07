import pandas as pd
import os
import json
import uuid
import csv
import requests
dataframes = {}
grand_error_set = {}
directory_name = './CSV_Files/'
directory = os.path.join(directory_name)

def input_files():
    for root,dirs,files in os.walk(directory):
        for file in files:
           if file.endswith(".csv"):
               dataframes[file] = (pd.read_csv(directory_name+file).dropna(axis=0, how='all'))

def add_companies(data):
    url = 'http://localhost:3004/web/v1/companyMapping/company'
    requests.post(url= url, data=data)

def add_instance(data):
    url = 'http://localhost:3004/web/v1/companyMapping/'
    requests.post(url = url, data=data)

def add_categories(obj):
    url = 'http://localhost:3004/web/v1/companyMapping/category'
    requests.post(url=url, data=obj)

def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)


def check_sub_category():
    for index, value in dataframes.items():
        try:
            df = value['Sub-Category']
        except:
            error_message = 'Column \'Sub-Category\' does not exist!'
            if (index in grand_error_set):
                grand_error_set[index].append(error_message)
            else:
                grand_error_set[index] = [error_message]


def check_link():
    for index, value in dataframes.items():
        try:
            df = value['Link']
            if (df.isnull().values.any()):
                error_message = 'Column \'Link\' has missing values'
                if (index in grand_error_set):
                    grand_error_set[index].append(error_message)
                else:
                    grand_error_set[index] = [error_message]
        except:
            error_message = 'Column \'Link\' does not exist!'
            if (index in grand_error_set):
                grand_error_set[index].append(error_message)
            else:
                grand_error_set[index] = [error_message]


def check_source():
    for index, value in dataframes.items():
        try:
            df = value['Sources']
            if (df.isnull().values.any()):
                error_message = 'Column \'Sources\' has missing values'
                if (index in grand_error_set):
                    grand_error_set[index].append(error_message)
                else:
                    grand_error_set[index] = [error_message]
        except:
            error_message = 'Column \'Sources\' does not exist!'
            if (index in grand_error_set):
                grand_error_set[index].append(error_message)
            else:
                grand_error_set[index] = [error_message]


def check_rationale():
    for index, value in dataframes.items():
        try:
            df = value['Rationale']
            if (df.isnull().values.any()):
                error_message = 'Column \'Rationale\' has missing values'
                if (index in grand_error_set):
                    grand_error_set[index].append(error_message)
                else:
                    grand_error_set[index] = [error_message]
        except:
            error_message = 'Column \'Rationale\' does not exist!'
            if (index in grand_error_set):
                grand_error_set[index].append(error_message)
            else:
                grand_error_set[index] = [error_message]

def check_keywords():
    for index, value in dataframes.items():
        try:
            df = value['Keywords']
            if (df.isnull().values.any()):
                error_message = 'Column \'Keywords\' has missing values'
                if (index in grand_error_set):
                    grand_error_set[index].append(error_message)
                else:
                    grand_error_set[index] = [error_message]
        except:
            error_message = 'Column \'Keywords\' does not exist!'
            if (index in grand_error_set):
                grand_error_set[index].append(error_message)
            else:
                grand_error_set[index] = [error_message]


def check_relevance_validity():
    for index, value in dataframes.items():
        try:
            df = value['Relevance']
            for i, j in df.iterrows():
                if not (j['Relevance'].lower() == 'high' or j['Relevance'].lower() == 'low'):
                    error_message = 'Column \'Relevance\' has invalid contents'
                    if (index in grand_error_set):
                        grand_error_set[index].append(error_message)
                    else:
                        grand_error_set[index] = [error_message]
        except:
            continue


def check_trend_validity():
    for index, value in dataframes.items():
        try:
            df = value['12M Trend']
            for i, j in df.iterrows():
                if not (j['12M Trend'] == '\+' or j['12M Trend'] == '\-' or j['12M Trend'] == '\='):
                    error_message = 'Column \'12M Trend\' has invalid contents'
                    if (index in grand_error_set):
                        grand_error_set[index].append(error_message)
                    else:
                        grand_error_set[index] = [error_message]
        except:
            continue


def check_relevance():
    for index, value in dataframes.items():
        try:
            df = value['Relevance']
            if (df.isnull().values.any()):
                error_message = 'Column \'Relevance\' has missing values'
                if (index in grand_error_set):
                    grand_error_set[index].append(error_message)
                else:
                    grand_error_set[index] = [error_message]
        except:
            error_message = 'Column \'Relevance\' does not exist!'
            if (index in grand_error_set):
                grand_error_set[index].append(error_message)
            else:
                grand_error_set[index] = [error_message]


def log_errors():
    check_company_names()
    check_category()
    check_sub_category()
    check_keywords()
    check_relevance()
    check_relevance_validity()
    check_trend()
    check_trend_validity()
    check_rationale()
    check_source()
    check_link()

def add_to_instance_database(df):
    for index, row in df.iterrows():
        print('For', index, get_company_name_from_ticker(row['Ticker']), row['Keywords'])
        if(index not in grand_error_set):
            send_to_add_instance = {}
            send_to_add_instance["category"] = row['Category']
            send_to_add_instance["country_of_incorporation"] = ''
            send_to_add_instance["company_name"] = get_company_name_from_ticker(row['Ticker'])
            if(pd.isna(row['Sub-Category']) == True):
                pass
            else:
                send_to_add_instance["sub_category"] = row['Sub-Category']
            send_to_add_instance["keyword"] = row['Keywords']
            send_to_add_instance["rationale"] = row['Rationale']
            send_to_add_instance["source"] = row['Sources']
            send_to_add_instance["link"] = row['Link']
            if(row['Relevance'].lower() == 'high'):
                send_to_add_instance["relevance"] = 1
            else:
                send_to_add_instance["relevance"] = 0 #todo figure out what happens if the index is neither high nor low
            if(row['12M Trend'] == '\+'):
                send_to_add_instance["trend"] = 2
            elif(row['12M Trend'] == '\='):
                send_to_add_instance["trend"] = 1
            else:
                send_to_add_instance["trend"] = 0 #todo figure out what happens if the index is neither + nor - nor =
            print(send_to_add_instance)
            add_instance(send_to_add_instance)


def check_trend():
    for index, value in dataframes.items():
        try:
            df = value['12M Trend']
            if (df.isnull().values.any()):
                error_message = 'Column \'12M Trend\' has missing values'
                if (index in grand_error_set):
                    grand_error_set[index].append(error_message)
                else:
                    grand_error_set[index] = [error_message]
        except:
            error_message = 'Column \'12M Trend\' does not exist!'
            if (index in grand_error_set):
                grand_error_set[index].append(error_message)
            else:
                grand_error_set[index] = [error_message]


def add_to_company_data_base(df): #TODO take unique tickers from dataframe and then find companies, much faster
    for index, row in df.iterrows():
        if(index not in grand_error_set):
            send_to_add_companies = {}
            send_to_add_companies["exchange"] = row['Exchange']
            send_to_add_companies["ticker"] = row['Ticker']
            send_to_add_companies["country_of_incorporation"] = ''
            send_to_add_companies["company_name"] = get_company_name_from_ticker(row['Ticker'])
            send_to_add_companies["create_uid"] = 34543
            send_to_add_companies["report_id"] = 1235
            print(send_to_add_companies)
            add_companies(send_to_add_companies)

def add_to_cat_data_base(df):
    for index, row in df.iterrows():
        if(index not in grand_error_set):
            send_to_add_category = {}
            send_to_add_category['category_name'] = row['Category']
            if(pd.isna(row['Sub-Category']) == True):
                send_to_add_category['is_sub'] = 0
            else:
                send_to_add_category['is_sub'] = 1
            add_categories(send_to_add_category)


def check_company_names():
    for index, value in dataframes.items():
        try:
            df = value[['Relevance', '12M Trend', 'Ticker', 'Exchange']]
            if (df.isnull().values.any()):
                #dataframe_company_set[index] = 'Columns \'Relevance or 12M Trend\' have missing values'
                if(index in grand_error_set):
                    grand_error_set[index].append('Columns \'Relevance or 12M Trend or Ticker or Exchange\' have missing values')
                else:
                    grand_error_set[index] = ['Columns \'Relevance or 12M Trend or Ticker or Exchange\' have missing values']
        except:
            if (index in grand_error_set):
                grand_error_set[index].append('Columns \'Relevance or 12M Trend or Ticker or Exchange\' don\'t exist')
            else:
                grand_error_set[index] = ['Columns \'Relevance or 12M Trend or Ticker or Exchange\' don\'t exist']


def check_category():
    for index, value in dataframes.items():
        try:
            df = value['Category']
            if (df.isnull().values.any()):
                error_message = 'Column \'Category\' has missing values'
                if (index in grand_error_set):
                    grand_error_set[index].append(error_message)
                else:
                    grand_error_set[index] = [error_message]
        except:
            error_message = 'Column \'Category\' does not exist!'
            if (index in grand_error_set):
                grand_error_set[index].append(error_message)
            else:
                grand_error_set[index] = [error_message]




def get_company_name_from_ticker(ticker):
    url = 'https://api.invbots.com/data/v1/stock/us/' + ticker+'/info'
    try:
        return (requests.get(url).json()[0]['name'])
    except:
        return ('')


def add_to_database():

    print('ADDING TO COMPANY DATABASE....')

    for index, df in dataframes.items():
        name = index
        name= name.replace('spx_companymapping - ', '')
        name= name.replace('.csv', '')
        print('For', name, end=', ')
        add_to_company_data_base(df.drop_duplicates(subset="Ticker"))
        print('added the dataframe to company database')

    print('ADDING TO CATEGORY DATABASE....')

    for index, df in dataframes.items():
        name = index
        name = name.replace('spx_companymapping - ', '')
        name = name.replace('.csv', '')
        print('For', name, end=', ')
        add_to_cat_data_base(df)
        print('added the dataframe to category database')

    print('ADDING TO INSTANCE DATABASE....')

    for index, df in dataframes.items():
        name = index
        name= name.replace('spx_companymapping - ', '')
        name= name.replace('.csv', '')
        if(name!='Communications Equipment'):
            print('For', name, end=', ')
            add_to_instance_database(df)
            print('added the dataframe to instance database')



if __name__ == "__main__":
    input_files()
    log_errors()
    print(grand_error_set)
    with open('grand_error_set.csv', 'w+') as f:
        for key in grand_error_set.keys():
            f.write("%s,%s\n" % (key, grand_error_set[key]))
    add_to_database()












