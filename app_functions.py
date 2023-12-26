import hashlib
from datetime import datetime, timedelta
import jwt
from integrations import *
import re
from datetime import datetime
import secrets

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
