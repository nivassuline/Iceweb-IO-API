import hashlib
from datetime import datetime, timedelta
import jwt
from integrations import *
import re
from datetime import datetime
import secrets
from sqlalchemy import text
from flask_socketio import SocketIO
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import threading


def camel_to_snake(column_name):
        result = [column_name[0].lower()]
        for char in column_name[1:]:
            if char.isupper():
                result.extend(['_', char.lower()])
            else:
                result.append(char)
        return ''.join(result)

def calculate_percentage(value, total, decimal_places=2):
    try:
        percentage = (value / total) * 100
        formatted_percentage = f"{percentage:.{decimal_places}f}"
        return formatted_percentage
    except ZeroDivisionError:
        return "Error: Total cannot be zero."
    

def generate_hashed_id(row):
    # Convert the row to a string and generate a unique hash
    row_string = str(row)
    hashed_id = hashlib.sha1(row_string.encode()).hexdigest()
    return hashed_id


def generate_api_key():
    new_api_key = secrets.token_urlsafe(32)

    return new_api_key


def generate_jwt_token(user_id, user_role,SECRET_KEY,api_key=None):
    # Define the payload of the token (typically contains user-related data)
    if api_key is not None:
        new_api_key = secrets.token_urlsafe(32)
        payload = {
            'user_id': user_id,
            'user_role': user_role,
            'api_key': new_api_key,
        }
    else:
        payload = {
            'user_id': user_id,
            'user_role': user_role,
            'exp': datetime.utcnow() + timedelta(hours=24)  # Token expiration time
        }

    # Generate the JWT token
    token = jwt.encode(payload, SECRET_KEY, algorithm='HS256')

    return token

def convert_to_user_friendly_age(age_str):
    # Convert the hour string to an integer
    age = float(age_str)

    if 0 <= age <= 9:
        return f"0 - 9"
    elif 10 <= age <= 19:
        return "10 - 19"
    elif 20 <= age <= 29:
        return "20 - 29"
    elif 30 <= age <= 39:
        return "30 - 39"
    elif 40 <= age <= 49:
        return "40 - 49"
    elif 50 <= age <= 59:
        return "50 - 59"
    elif 60 <= age <= 69:
        return "60 - 69"
    elif 70 <= age <= 79:
        return "70 - 79"
    elif 80 <= age <= 89:
        return "80 - 89"
    elif 90 <= age <= 99:
        return "90 - 99"
    elif age >= 100:
        return "100+"
    

def convert_to_user_friendly_time(hour_str):
    # Convert the hour string to an integer
    hour = int(hour_str)

    if 1 <= hour <= 11:
        return f"{hour} AM"
    elif hour == 12:
        return f"{hour} PM"
    elif 13 <= hour <= 23:
        return f"{hour - 12} PM"
    elif hour == 0:
        return "12 AM"
    else:
        return "Invalid Hour"


def get_integration(integration_name):
    if integration_name == 'Klaviyo':
        return klayviyo_integration
    elif integration_name == 'Customer.io':
        return customerIO_integration
    elif integration_name == 'Mailchimp':
        return mailchimp_integration
    elif integration_name == 'SendGrid':
        return sendgrid_integration
    elif integration_name == 'HubSpot':
        return hubspot_integration
    elif integration_name == 'EmailOctopus':
        return emailoctopus_integration
    elif integration_name == 'OmniSend':
        return omnisend_integration


def convert_currency_string_to_float(currency_string):
    if isinstance(currency_string, (int, float)):
        return currency_string

    # Remove dollar sign and commas, then convert to float
    return float(re.sub(r'[^\d.]', '', currency_string))

def calculate_percentage_change(original_value, new_value):
    original_value = convert_currency_string_to_float(original_value)
    new_value = convert_currency_string_to_float(new_value)
    
    if original_value == 0:
        return "100.00%"
    percentage_change = ((new_value - original_value) / abs(original_value)) * 100.0
    return f"{percentage_change:.2f}%"

def add_log(collection,endpoint,request_data,response,user_id=None,company_id=None,segment_id=None):
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

        log = {
            "endpoint" : endpoint,
            "request_data" : request_data,
            "response" : response,
            "date_time": dt_string
        }
        if user_id:
            log["user_id"] = user_id
        if company_id:
            log["company_id"] = company_id
        if segment_id:
            log["segment_id"] = segment_id

        collection.insert_one(log)


def process_row(row, integration_name, segment_id,SEGMENT_COLLECTION,USER_COLLECTION,user_id,index,num_rows,api_key,client_secret,site_id,public_id,list_id,shared_data,stop_threads):
        if shared_data["stop_threads"] == True:
            print('stop')
            return 'stop'
        if segment_id is not None:
            if SEGMENT_COLLECTION.find_one({'_id': segment_id})["integrations"][index]["sync_started"] == 0:
                    SEGMENT_COLLECTION.find_one_and_update({'_id': segment_id}, {'$set': {f"integrations.{index}.sync_started" : 0}})
                    SEGMENT_COLLECTION.find_one_and_update({'_id': segment_id}, {'$set': {f"integrations.{index}.sync_progress" : 0}})
                    shared_data["stop_threads"] = True
                    return 0
        else:
            if USER_COLLECTION.find_one({'_id': user_id})["integrations"][index]["sync_started"] == 0:
                    USER_COLLECTION.find_one_and_update({'_id': user_id}, {'$set': {f"integrations.{index}.sync_started" : 0}})
                    USER_COLLECTION.find_one_and_update({'_id': user_id}, {'$set': {f"integrations.{index}.sync_progress" : 0}})
                    shared_data["stop_threads"] = True
                    return 0
        
        #adding each row to the row num
        with shared_data['row_num_lock']:
            shared_data['row_num'] += 1
            print(shared_data['row_num'])
            percentage = int((shared_data['row_num'] / num_rows) * 100)
            print(percentage)

        if integration_name == 'Klaviyo':
            klayviyo_integration(api_key,row) 
        elif integration_name == 'BayEngage':
            bayengage_integration(client_secret,public_id,row,list_id)
        elif integration_name == 'Customer.io':
            customerIO_integration(site_id,api_key,row) 
        elif integration_name == 'Mailchimp':
            mailchimp_integration(api_key,list_id,row) 
        elif integration_name == 'SendGrid':
            sendgrid_integration(api_key,row,[list_id]) 
        elif integration_name == 'HubSpot':
            hubspot_integration(api_key,row) 
        elif integration_name == 'EmailOctopus':
            emailoctopus_integration(api_key,list_id,row) 
        elif integration_name == 'OmniSend':
            omnisend_integration(api_key,row)
        
        if segment_id is not None:
            SEGMENT_COLLECTION.find_one_and_update({'_id': segment_id}, {'$set': {f"integrations.{index}.sync_progress" : percentage}})
        else:
            USER_COLLECTION.find_one_and_update({'_id': user_id}, {'$set': {f"integrations.{index}.sync_progress" : percentage}})


def sync_integration(ENGINE,USER_COLLECTION,company_id,user_id,index,api_key,integration_name,client_secret=None,public_id=None,site_id=None,list_id=None,filter_query=None,segment_id=None,SEGMENT_COLLECTION=None):
    stop_threads = False
    # Build and execute the SQL query to select a random row
    if segment_id is not None:
        SEGMENT_COLLECTION.find_one_and_update({'_id': segment_id}, {'$set': {f"integrations.{index}.sync_started" : 1}})
        query = text(f"SELECT DISTINCT ON (full_name) * FROM {company_id} WHERE {filter_query};")
    else:
        USER_COLLECTION.find_one_and_update({'_id': user_id}, {'$set': {f"integrations.{index}.sync_started" : 1}})
        query = text(f"SELECT DISTINCT ON (full_name) * FROM {company_id}")
    
    shared_data = {'row_num': 0, 'row_num_lock': threading.Lock(),'stop_threads' : False}  # Shared data dictionary



    with ENGINE.connect() as connection:
        result = connection.execute(query)
        column_names = result.keys()
        num_rows = result.rowcount

    data = [dict(zip(column_names, row)) for row in result]
    

    with ThreadPoolExecutor() as executor:
        futures = []

        for row in data:
            future = executor.submit(process_row, row,integration_name,segment_id,SEGMENT_COLLECTION,USER_COLLECTION,user_id,index,num_rows,api_key,client_secret,site_id,public_id,list_id,shared_data,stop_threads)
            futures.append(future)

        # Wait for all threads to finish
        concurrent.futures.wait(futures)


    if segment_id is not None:
        SEGMENT_COLLECTION.find_one_and_update({'_id': segment_id}, {'$set': {f"integrations.{index}.sync_started" : 0}})
        SEGMENT_COLLECTION.find_one_and_update({'_id': segment_id}, {'$set': {f"integrations.{index}.sync_progress" : 0}})
        return 'Done'
    
    else:
        USER_COLLECTION.find_one_and_update({'_id': user_id}, {'$set': {f"integrations.{index}.sync_started" : 0}})
        USER_COLLECTION.find_one_and_update({'_id': user_id}, {'$set': {f"integrations.{index}.sync_progress" : 0}})
        return 'Done'
    
