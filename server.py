from datetime import datetime, timedelta
import apscheduler.jobstores.base
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.combining import OrTrigger
from apscheduler.triggers.cron import CronTrigger
from flask import Flask, render_template, request, redirect, session, url_for, jsonify, send_file, Response, send_from_directory
from flask_bcrypt import Bcrypt
from urllib.parse import urlparse
from pymongo import MongoClient
from bson import json_util
from datetime import datetime, date
import pandas as pd
from flask_cors import CORS
import random
import json
import hashlib
import uuid
import jwt
import csv
import re
import threading
import concurrent.futures
import os


app = Flask(__name__)
CORS(app)
bcrypt = Bcrypt(app)
scheduler = BackgroundScheduler()
scheduler.start()



# DB_CONNECTOR = MongoClient("mongodb://icewebniv:G3ccZpGQXvs6mVdsYuTCEfk3EuDKTdASUsNyEi6HCFoFp7Af3ESn0asd80pDRIP1w51FILE3QvdYACDbYY0n4g==@icewebniv.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@icewebniv@")
DB_CONNECTOR = MongoClient(
    "mongodb://icwebio:yJtwZocIwp8tZ4CQGAnRVvBxCv2i3bGIDHa3Za7w6sP3ww5I5Bgm8e1OpSSMbvdLIpEh7KDeWX4WACDbtUkLGw==@icwebio.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@icwebio@")
DB_CLIENT = DB_CONNECTOR['main']
ADMIN_DB = DB_CONNECTOR
DB_CLIENT.command("enableSharding", "main")
COMPANIES_COLLECTION = DB_CLIENT['companies']
USER_COLLECTION = DB_CLIENT['users']
ROLE_COLLECTION = DB_CLIENT['roles']
SEGMENT_COLLECTION = DB_CLIENT['segments']
SECRET_KEY = 'iceweb123456789'
TRIGGER = OrTrigger([CronTrigger(hour=17, minute=0)])

DESIRED_COLUNMS_ORDER = ["date", "id", "hour", "fullName", "firstName", "lastName", "url", "facebook", "linkedIn", "twitter", "email", "optIn", "optInDate", "optInIp", "optInUrl", "pixelFirstHitDate", "pixelLastHitDate", "bebacks", "phone", "dnc", "age", "gender", "maritalStatus", "address", "city", "state", "zip", "householdIncome", "netWorth", "incomeLevels", "peopleInHousehold", "adultsInHousehold", "childrenInHousehold", "veteransInHousehold", "education", "creditRange", "ethnicGroup", "generation", "homeOwner", "occupationDetail", "politicalParty", "religion", "childrenBetweenAges0_3", "childrenBetweenAges4_6", "childrenBetweenAges7_9",
                         "childrenBetweenAges10_12", "childrenBetweenAges13_18", "behaviors", "childrenAgeRanges", "interests", "ownsAmexCard", "ownsBankCard", "dwellingType", "homeHeatType", "homePrice", "homePurchasedYearsAgo", "homeValue", "householdNetWorth", "language", "mortgageAge", "mortgageAmount", "mortgageLoanType", "mortgageRefinanceAge", "mortgageRefinanceAmount", "mortgageRefinanceType", "isMultilingual", "newCreditOfferedHousehold", "numberOfVehiclesInHousehold", "ownsInvestment", "ownsPremiumAmexCard", "ownsPremiumCard", "ownsStocksAndBonds", "personality", "isPoliticalContributor", "isVoter", "premiumIncomeHousehold", "urbanicity", "maid", "maidOs"]
ITEMS_PER_PAGE = 13



def generate_hashed_id(row):
    # Convert the row to a string and generate a unique hash
    row_string = str(row)
    hashed_id = hashlib.sha1(row_string.encode()).hexdigest()
    return hashed_id


def insert_chunk(collection, records):
    collection.insert_many(records)


def generate_jwt_token(user_id, user_role):
    # Define the payload of the token (typically contains user-related data)
    payload = {
        'user_id': user_id,
        'user_role': user_role,
        'exp': datetime.utcnow() + timedelta(hours=24)  # Token expiration time
    }

    # Generate the JWT token
    token = jwt.encode(payload, SECRET_KEY, algorithm='HS256')

    return token

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

def get_most_popular(collection, date_range_query, popular_type):
    item_list = []
    count_list = []

    if popular_type == 'hour':

        project_stage = {
            "$project": {
                "hour": {
                    "$substr": ["$hour", 0, 2]  # Extract the first 2 characters (the hour part)
                }
            }
        }

        pipeline = [
            {
                "$match": date_range_query
            },
            project_stage,  # Add the project stage to extract the hour part
            {
                "$group": {
                    "_id": "$hour",  # Group by the extracted hour
                    "count": {"$sum": 1}  # Count occurrences of each hour
                }
            },
            {
                "$sort": {"count": -1}  # Sort by count in descending order
            },
            {
                "$limit": 10
            }
        ]
        result = list(collection.aggregate(pipeline))

        for item in result:
            item_list.append(convert_to_user_friendly_time(item['_id']))
            count_list.append(item['count'])

        return item_list, count_list
    else:
         pipeline = [
        {
            "$match":
            date_range_query

        },
        {
            "$group": {
                "_id": f"${popular_type}",  # Group by the field you want to count
                "count": {"$sum": 1}  # Count occurrences of each value
            }
        },
        {
            "$sort": {"count": -1}  # Sort by count in descending order
        },
        {
            "$limit": 10
        }
        ]
    result = list(collection.aggregate(pipeline))

    for item in result:
        item_list.append(item['_id'])
        count_list.append(item['count'])

    return item_list, count_list


def get_by_precent_count(collection, field, date_range_query):
    item_list = []
    count_list = []

    pipeline = [
        {
            "$match":
            date_range_query

        },
        {
            "$group": {
                "_id": f"${field}",  # Group by the field you want to count
                "count": {"$sum": 1}  # Count occurrences of each value
            }
        },
        {
            "$sort": {"count": -1}  # Sort by count in descending order
        },
        {
            "$limit": 10
        }
        ]
    
    result = list(collection.aggregate(pipeline))
    for item in result:
        if item['_id'] == '-' or item['_id'] == 'Unknown':
            pass
        else:
            item_list.append(item['_id'])
            count_list.append(item['count'])

    return item_list, count_list


def get_counts(collection, date_range_query=None):
    if date_range_query:
        pipeline = [
            {
                "$match": date_range_query
            },
            {
                "$group": {
                    "_id": "$fullName"
                }
            },
            {
                "$count": "total_distinct_count"
            }
        ]

        result = list(collection.aggregate(pipeline))
        journey_count = collection.count_documents(date_range_query)
        try:
            people_count = result[0]["total_distinct_count"]
        except IndexError:
            people_count = 0
    else:
        pipeline_people = [
            {
                "$group": {
                    "_id": "$fullName"
                }
            },
            {
                "$count": "total_distinct_count"
            }
        ]
        pipeline_journey = [
            {
                "$group": {
                    "_id": None,
                    "count": {"$sum": 1}
                }
            }
        ]

        result_people = list(collection.aggregate(pipeline_people))
        result_journey = list(collection.aggregate(pipeline_journey))
        try:
            people_count = result_people[0]["total_distinct_count"]
            journey_count = result_journey[0]["count"]

        except IndexError:
            people_count = 0
            journey_count = 0

    return journey_count, people_count


def get_date_query(start_date, end_date, search=None):

    if any([start_date == 'undefined', start_date == None]):
        start_datetime_to_obj = datetime.today()
        start_datetime = datetime.strftime(start_datetime_to_obj, '%Y-%m-%d')
        end_datetime_to_obj = datetime.strptime(
            f'{datetime.now().year}-01-01', '%Y-%m-%d')
        end_datetime = datetime.strftime(end_datetime_to_obj, '%Y-%m-%d')
        date_range_query = {
            'date': {
                '$gte': end_datetime,
                '$lte': start_datetime
            }
        }
    else:
        # Parse the start_date and end_date as datetime objects
        start_datetime_to_obj = datetime.strptime(start_date, '%Y-%m-%d')
        start_datetime = datetime.strftime(start_datetime_to_obj, '%Y-%m-%d')
        end_datetime_to_obj = datetime.strptime(end_date, '%Y-%m-%d')
        end_datetime = datetime.strftime(end_datetime_to_obj, '%Y-%m-%d')

        # Create a query to find documents within the specified date range
        date_range_query = {
            'date': {
                '$gte': start_datetime,
                '$lte': end_datetime
            }
        }

    return date_range_query


def build_filter(filter_params=None, column_filters=None):
    filters = []
    if filter_params:
        for filter in filter_params:
            filter_key = filter["id"]
            filter_value = filter["value"]
            if filter_key == 'range':
                column_name = filter_value['name']
                min_val = int(filter_value['minVal'])
                max_val = int(filter_value['maxVal'])

                filters.append(
                    {column_name: {'$gte': min_val, '$lte': max_val}})
            elif filter_key == 'contains':
                or_arr = []
                for include in filter_value['include_array']:
                    try:
                        column_name = 'url'
                        include_value = include['value']
                        include_type = include['type']
                        if include_type == 'contain':
                            or_arr.append(
                                {column_name: {"$regex": include_value, "$options": 'i'}})
                        elif include_type == 'start':
                            or_arr.append(
                                {column_name: {"$regex": f'^{include_value}', "$options": 'i'}})
                        elif include_type == 'end':
                            or_arr.append(
                                {column_name: {"$regex": f'{include_value}$', "$options": 'i'}})
                        elif include_type == 'exact':
                            or_arr.append(
                                {column_name: include_value})
                        elif include_type == 'notequal':

                            regex_pattern = f"{re.escape(include_value)}"
                            

                            or_arr.append(
                                {column_name: {"$regex": f'{regex_pattern}', "$not": {"$regex": fr"\{include_value}$"}, "$options": 'i'}})
                    except KeyError:
                        pass
                        
                if len(or_arr) > 0:
                    filters.append({'$or': or_arr})

            elif filter_key == 'checkbox':
                column_name = filter_value['name']
                value_arr = filter_value['value']
                or_arr = []

                for value in value_arr:
                    value = re.escape(value)
                    or_arr.append({column_name: {'$regex': value}})

                filters.append({'$or': or_arr})
            else:
                filters.append(
                    {filter_key: {"$regex": filter_value, "$options": 'i'}})


    if column_filters:
        for filter in column_filters:
            filter_key = filter["id"]
            filter_value = filter["value"]
            filters.append(
                {filter_key: {"$regex": filter_value, "$options": 'i'}})

    # Combine filters using '$and' for multiple filters
    if filters:
        return {'$and': filters}
    else:
        return {}  # No filters
    



def update_company_counts(company_id):
    company = COMPANIES_COLLECTION.find_one({'_id': company_id})
    if company:
        company_collection = DB_CLIENT[f'{company_id}_data']
        pipeline_people = [
            {
                "$group": {
                    "_id": "$fullName"
                }
            },
            {
                "$count": "total_distinct_count"
            }
        ]
        pipeline_journey = [
            {
                "$group": {
                    "_id": None,
                    "count": {"$sum": 1}
                }
            }
        ]

        result_people = list(company_collection.aggregate(pipeline_people))
        result_journey = list(company_collection.aggregate(pipeline_journey))
        try:
            people_count = result_people[0]["total_distinct_count"]
            journey_count = result_journey[0]["count"]

        except IndexError:
            people_count = 0
            journey_count = 0

        COMPANIES_COLLECTION.update_one({'_id': company_id}, {'$set' : {'counts' : {
            "people_count" : people_count,
            "journey_count" : journey_count
        }}})
    else:
        print("Company not found or an error occurred.")

def update_popular_chart(company_id):
    popular_chart = {}
    popular_types = ['hour', 'url']
    collection = DB_CLIENT[f'{company_id}_data']

    for popular_type in popular_types:

        if popular_type == 'hour':
            item_list = []
            count_list = []
            project_stage = {
                "$project": {
                    "hour": {
                        "$substr": ["$hour", 0, 2]  # Extract the first 2 characters (the hour part)
                    }
                }
            }

            pipeline = [
                project_stage,  # Add the project stage to extract the hour part
                {
                    "$group": {
                        "_id": "$hour",  # Group by the extracted hour
                        "count": {"$sum": 1}  # Count occurrences of each hour
                    }
                },
                {
                    "$sort": {"count": -1}  # Sort by count in descending order
                },
                {
                    "$limit": 10
                }
            ]
            result = list(collection.aggregate(pipeline))

            for item in result:
                item_list.append(convert_to_user_friendly_time(item['_id']))
                count_list.append(item['count'])

            popular_chart[popular_type] = {
                'item_list' : item_list,
                'count_list' : count_list
            }

        elif popular_type == 'url':
            item_list = []
            count_list = []
            pipeline = [
            {
                "$group": {
                    "_id": f"${popular_type}",  # Group by the field you want to count
                    "count": {"$sum": 1}  # Count occurrences of each value
                }
            },
            {
                "$sort": {"count": -1}  # Sort by count in descending order
            },
            {
                "$limit": 10
            }
            ]
            result = list(collection.aggregate(pipeline))

            for item in result:
                item_list.append(item['_id'])
                count_list.append(item['count'])
            
            popular_chart[popular_type] = {
                        'item_list' : item_list,
                        'count_list' : count_list
                    }
    COMPANIES_COLLECTION.update_one({'_id': company_id}, {'$set' : {
        'popular_chart' : popular_chart
    }})

def update_by_precent(company_id):
    collection = DB_CLIENT[f'{company_id}_data']
    by_precent_chart = {}
    fields = ['age','gender']
    for field in fields:
        item_list = []
        count_list = []
        pipeline = [
            {
                "$group": {
                    "_id": f"${field}",  # Group by the field you want to count
                    "count": {"$sum": 1}  # Count occurrences of each value
                }
            },
            {
                "$sort": {"count": -1}  # Sort by count in descending order
            },
            {
                "$limit": 10
            }
            ]
        
        result = list(collection.aggregate(pipeline))
        for item in result:
            if item['_id'] == '-' or item['_id'] == 'Unknown':
                pass
            else:
                item_list.append(item['_id'])
                count_list.append(item['count'])

        by_precent_chart[field] = {
            'item_list': item_list,
            'count_list': count_list
        }
    COMPANIES_COLLECTION.update_one({'_id': company_id}, {'$set' : {
        'by_precent_chart' : by_precent_chart
    }})
def update_excluded_users(segment_id):
    segment = SEGMENT_COLLECTION.find_one({'_id': segment_id})
    if segment:
        filters = segment['filters']
        company_id = segment['attached_company']
        collection = DB_CLIENT[f'{company_id}_data']
        user_ids_to_exclude = set()

        # Compile regex patterns for different types outside the loop
        regex_patterns = {
            'contain': (lambda value: re.compile(re.escape(value), re.IGNORECASE)),
            'start': (lambda value: re.compile(f"^{re.escape(value)}", re.IGNORECASE)),
            'end': (lambda value: re.compile(f"{re.escape(value)}$", re.IGNORECASE)),
        }

        for filter_item in filters:
            if filter_item["id"] == 'contains':
                exclude_array = filter_item.get("value", {}).get('exclude_array', [])
                querys = []
                for exclude in exclude_array:
                    exclude_value = exclude.get('value')
                    exclude_type = exclude.get('type', 'contain')
                    exclude_regex = regex_patterns.get(exclude_type, regex_patterns['contain'])(exclude_value)
                    querys.append({"url": {"$regex": exclude_regex}})
                excluded_users = collection.find({"$or": querys})
                user_ids_to_exclude.update(user.get('fullName') for user in excluded_users)

        SEGMENT_COLLECTION.update_one(
            {'_id': segment_id},
            {'$set': {'excluded_users': list(user_ids_to_exclude)}}
        )
    else:
        print("Segment not found or an error occurred.")



for segment in SEGMENT_COLLECTION.find():
    scheduler.add_job(id=segment['_id'], 
                      func=update_excluded_users,
                      trigger=OrTrigger([CronTrigger(hour=18, minute=30)]), 
                      misfire_grace_time=15*60,
                      args=[segment['_id']]
                        )

for company in COMPANIES_COLLECTION.find():
    scheduler.add_job(
        func=update_company_counts,
        trigger=OrTrigger([CronTrigger(hour=18, minute=0)]), 
        misfire_grace_time=15*60,
        args=[company['_id']]
    )
    scheduler.add_job(
        func=update_popular_chart,
        trigger=OrTrigger([CronTrigger(hour=19, minute=0)]), 
        misfire_grace_time=15*60,
        args=[company['_id']]
    )
    scheduler.add_job(
        func=update_by_precent,
        trigger=OrTrigger([CronTrigger(hour=19, minute=0)]), 
        misfire_grace_time=15*60,
        args=[company['_id']]
    )







@app.route("/api/test", methods=['GET'])
def tes():
    # scheduler.print_jobs()

    file_path = '/company_data/data.csv'

    if os.path.exists(file_path):
        return jsonify('yes!')
    else:
        return jsonify(f'The file {file_path} does not exist.')

    return jsonify('done!')

@app.route("/api/login", methods=['POST'])
def login():
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    user = USER_COLLECTION.find_one({'user_email': email})

    if user and bcrypt.check_password_hash(user['user_password'], password):
        token = generate_jwt_token(user["_id"], user["user_role"])
        # Password is correct
        # You can generate a token here and include it in the response for future authenticated requests
        response = {
            'message': 'Login successful',
            # Convert ObjectId to string for JSON serialization
            'user_id': user['_id'],
            'token': token
        }
        return jsonify(response), 200
    else:
        # Invalid credentials
        return jsonify({'error': 'Login failed'}), 401  # Unauthorized


@app.route("/api/register", methods=['POST'])
def register():
    content = request.get_json()
    user_email = content.get('user_email')
    user_name = content.get('user_name')
    user_password = content.get('user_password')
    user_role = content.get('user_role')

    # Hash the password before storing it in the database
    hashed_password = bcrypt.generate_password_hash(
        user_password).decode('utf-8')

    # Check if the email already exists in the database
    if USER_COLLECTION.find_one({'user_email': user_email}):
        return jsonify({'error': 'Email already exists'}), 400  # Bad Request

    companies_list = []
    if user_role == 'admin':
        companies = COMPANIES_COLLECTION.find({})
        for company in companies:
            companies_list.append(company["_id"])

    # Create a new user document in the database
    new_user = {
        '_id': uuid.uuid4().hex,
        'user_email': user_email,
        'user_name': user_name,
        'user_password': hashed_password,
        'user_role': user_role,
        'companies': companies_list
    }

    USER_COLLECTION.insert_one(new_user)

    return jsonify({'message': 'Registration successful'}), 201  # Created


@app.route('/api/add-company', methods=['POST'])
def add_company():
    content = request.get_json()
    company_tools_id = content.get('company_tools_id')
    company_name = content.get('company_name')
    company_email = content.get('company_email')
    attached_users = content.get('attached_users')

    if len(attached_users[0]) < 1:
        attached_users.clear()

    # Check if the email already exists in the database
    if COMPANIES_COLLECTION.find_one({'company_email': company_email}):
        return jsonify({'error': 'Email already exists'}), 400  # Bad Request

    # Hash the password before storing it in the database
    company_id = f'{content["company_name"][0]}{content["company_name"][-1]}{random.randint(100,999)}'
    # Create a new user document in the database
    new_company = {
        '_id': company_id.lower(),
        'company_tools_id': company_tools_id,
        'company_name': company_name,
        'company_email': company_email,
        'attached_users': attached_users

        # Add other user-related daata as needed
    }

    USER_COLLECTION.update_many({"user_role": "admin"}, {
                                "$push": {"companies": company_id}})
    COMPANIES_COLLECTION.insert_one(new_company)



    return jsonify({'message': 'Registration successful'}), 201  # Created


@app.route("/api/add-data", methods=['POST'])
def add_data():
    company_id = request.args.get('id')
    company_collection = DB_CLIENT[f'{company_id}_data']

    df = pd.read_csv(
        '/Users/neevassouline/Desktop/Coding Projects/IcewebIO/backend/people_data_20.csv')
    df.fillna('-', inplace=True)

    df[['date', 'hour']] = df['date'].str.split(' ', expand=True)
    df[['hour']] = df['hour'].str.split('+', expand=True)[[0]]
    df['fullName'] = df['firstName'] + ' ' + df['lastName']

    list_of_lists = df.to_dict(orient='records')
    company_collection.insert_many(list_of_lists)

    return jsonify('Done!')


@app.route("/api/add-segment", methods=['POST'])
def add_segment():
    token = request.headers.get('Authorization')
    try:
        data = request.get_json()
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
        company_id = data.get('company_id')
        segment_name = data.get('segment_name')
        filters = data.get('filters')
        excluded_users = data.get('excluded_users', [])
        

        user = USER_COLLECTION.find_one({'_id': user_id})

        segment_id = uuid.uuid4().hex

        new_segment = {
            "_id": segment_id,
            "segment_name": segment_name,
            "attached_company": company_id,
            "created_by": user["user_name"],
            "filters": filters,
            "excluded_users": excluded_users
        }

        SEGMENT_COLLECTION.insert_one(new_segment)

        scheduler.add_job(id=segment_id, func=update_excluded_users,trigger=CronTrigger(hour='*/10'), misfire_grace_time=15*60,
                            args=[segment_id])

        print(scheduler.get_job(segment_id))
        return jsonify('Done!')
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/delete-user", methods=['POST'])
def delete_user():
    data = request.get_json()

    user_id = data.get('user_id')
    user = USER_COLLECTION.find_one({'_id': user_id})
    USER_COLLECTION.delete_one({'_id': user_id})
    COMPANIES_COLLECTION.update_many({'attached_users': {'$regex': user['user_email']}}, {
                                     '$pull': {'attached_users': user['user_email']}})

    return jsonify('done!')


@app.route("/api/delete-company", methods=['POST'])
def delete_company():
    data = request.get_json()

    company_id = data.get('company_id')

    COMPANIES_COLLECTION.delete_one({'_id': company_id})
    USER_COLLECTION.update_many({'companies': {'$regex': company_id}}, {
                                '$pull': {'companies': company_id}})

    return jsonify("done!")


@app.route("/api/delete-segment", methods=['POST'])
def delete_segment():
    data = request.get_json()

    segment_id = data.get('segment_id')

    SEGMENT_COLLECTION.delete_one({'_id': segment_id})

    print(scheduler.get_job(segment_id))

    scheduler.remove_job(segment_id)

    return jsonify("done!")


@app.route("/api/import", methods=['POST'])
def import_data():
    company_id = request.args.get('id')
    company_collection = DB_CLIENT[f'{company_id}_data']
    path = '/Users/neevassouline/Desktop/Coding Projects/IcewebIO/backend/data.csv'

    df = pd.read_csv(path)
    df.fillna('-', inplace=True)

    df["date"] = pd.to_datetime(df["date"])
    df['hour'] = df['date'].dt.strftime('%H:%M:%S')
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df['fullName'] = df['firstName'] + ' ' + df['lastName']
    df['url'] = df['url'].apply(lambda x: x.replace(
        f'{urlparse(x).scheme}://{urlparse(x).netloc}', ''))

    # Rearrange columns in the desired order
    df_filtered = df[DESIRED_COLUNMS_ORDER]
    # df_filtered['hashed_id'] = df_filtered.apply(generate_hashed_id, axis=1)

    # list_of_lists = df_filtered.to_dict(orient='records')
    # company_collection.insert_many(list_of_lists)
    admin_db = DB_CONNECTOR['admin']

    try:
        shard_key = {"id": "hashed"}
        admin_db.command("enableSharding", "main")
        admin_db.command("shardCollection",
                         f"main.{company_id}_data", key=shard_key)
    except Exception:
        print('asd')

    chunk_size = 100
    num_threads = 4
    l = len(df_filtered)
    i = 0
    threads = []

    while i < l:
        chunk = df_filtered.iloc[i:i+chunk_size]
        records = chunk.to_dict('records')

        if len(threads) >= num_threads:
            # Wait for running threads to complete
            for thread in threads:
                thread.join()
            threads = []

        thread = threading.Thread(
            target=insert_chunk, args=(company_collection, records))
        thread.start()
        threads.append(thread)

        i += chunk_size

    # Wait for any remaining threads to complete
    for thread in threads:
        thread.join()

    return jsonify('Done!')


@app.route("/api/get-company-list", methods=['POST'])
def get_company_list():
    companies = []
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']

        user = USER_COLLECTION.find_one({'_id': user_id})
        companies_arr = user['companies']

        for id in companies_arr:
            id = id.lower()
            company = COMPANIES_COLLECTION.find_one({"_id": id})
            if company:
                counts = company['counts']
                companies.append({
                    "company_details": company,
                    "counts": {
                        "journey_count": counts['journey_count'],
                        "people_count": counts['people_count']
                    }
                })

        return jsonify(companies), 200
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/get-user-list", methods=['POST'])
def get_user_list():
    users_lst = []
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']

        users = USER_COLLECTION.find({})

        for user in users:
            users_lst.append({
                "user_name": user['user_name'],
                "user_id": user['_id']
            })

        return jsonify(users_lst), 200
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/get-segment-list", methods=['POST'])
def get_segment_list():
    segment_list = []
    token = request.headers.get('Authorization')
    try:
        data = request.get_json()
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
        company_id = data.get("company_id")

        company_collection = DB_CLIENT[f'{company_id}_data']
        segments = SEGMENT_COLLECTION.find({"attached_company": company_id})

        for segment in segments:
            filter_query = build_filter(segment["filters"])
            people_count = get_counts(company_collection, filter_query)[1]
            segment_list.append({
                "segment_id": segment["_id"],
                "segment_name": segment["segment_name"],
                "created_by": segment["created_by"],
                "people_count": people_count,
                "filters": segment["filters"],
                "excluded_users": segment['excluded_users']

            })

        if len(segment_list) > 0:
            return jsonify(segment_list), 200
        else:
            return jsonify('Not Found')
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/get-data", methods=['POST'])
def get_users():
    data = request.get_json()
    company_id = data.get('id').lower()
    start_date = data.get('start-date')
    end_date = data.get('end-date')
    page = data.get('page')
    search = data.get('search')
    column_filters = data.get('column_filter')
    filters = data.get('filters')
    sorting = data.get('sorting')
    segment_id = data.get('segment_id')
    is_segnew = False
    excluded_users = data.get('excluded_users')
    user_ids_to_exclude = None


    try:
        if len(filters) > 0:
            filter_query = build_filter(filters, column_filters)

        elif len(filters) == 0:
            return jsonify('ignore')
    except TypeError:
        try:
            if filters[0] == 'segnew':
                filter_query = build_filter(filter_params=filters[1])
                is_segnew = True
                search = 'false'
        except IndexError:
            filter_query = build_filter(
                filter_params=None, column_filters=column_filters)
        except TypeError:
            filter_query = build_filter(
                filter_params=None, column_filters=column_filters)


    collection = DB_CLIENT[f'{company_id}_data']

    date_range_query = get_date_query(start_date, end_date)

    skip = page * ITEMS_PER_PAGE


    if search != 'false':
        regex_search = re.compile(search.lower(), re.IGNORECASE)
        base_query = {
            '$and': [
                {'fullName': {"$regex": regex_search}},
                date_range_query,
                filter_query
            ]
        }
    else:
        base_query = {
            '$and': [date_range_query, filter_query]
        }

        if excluded_users:
            base_query['fullName'] = {"$nin": excluded_users}
        else:
            if is_segnew:
                filters = filters[1]
            if filters:
                try:
                    user_ids_to_exclude = set()
                    # Compile regex patterns for different types outside the loop
                    regex_patterns = {
                        'contain': (lambda value: re.compile(re.escape(value), re.IGNORECASE)),
                        'start': (lambda value: re.compile(f"^{re.escape(value)}", re.IGNORECASE)),
                        'end': (lambda value: re.compile(f"{re.escape(value)}$", re.IGNORECASE)),
                    }

                    user_ids_to_exclude = set()

                    for filter in filters:
                        if filter["id"] == 'contains':
                            querys = []
                            exclude_array = filter["value"].get('exclude_array', [])
                            for exclude in exclude_array:
                                exclude_value = exclude['value']
                                exclude_type = exclude['type'] or 'contain'
                                exclude_regex = regex_patterns.get(exclude_type, regex_patterns['contain'])(exclude_value)
                                querys.append({"url": {"$regex": exclude_regex}})
                            excluded_users = collection.find({"$or": querys})
                            user_ids_to_exclude.update(user['fullName'] for user in excluded_users)

                    if user_ids_to_exclude:
                        base_query['fullName'] = {"$nin": list(user_ids_to_exclude)}
                except KeyError:
                    pass

    instance = collection.find(base_query).skip(skip).limit(ITEMS_PER_PAGE)

    # Apply sorting if provided
    if sorting:
        # Add sorting logic based on the sorting parameter
        for option in sorting:
            if option["desc"] == "False":
                instance = instance.sort([(option["id"], 1)])
            elif option["desc"] == "True":
                instance = instance.sort([(option["id"], -1)])
    
    if user_ids_to_exclude:
        response_data = {
            'users': json.loads(json_util.dumps(list(instance))),
            'excluded_users': list(user_ids_to_exclude)
        }
    else:
        response_data = {
            'users': json.loads(json_util.dumps(list(instance)))
        }

    return jsonify(response_data)


@app.route("/api/get-user-details", methods=['POST'])
def get_user_details():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
        user = USER_COLLECTION.find_one({"_id": user_id})

        response = {
            "user_email": user["user_email"],
            "user_name": user["user_name"],
            "user_role": user["user_role"],
        }
        return response, 200
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/get-company-counts", methods=['POST'])
def get_company_counts():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])

        user_id = payload['user_id']

        company_id = request.json.get('company_id')
        start_date = request.json.get('start_date')
        end_date = request.json.get('end_date')

        company_collection = DB_CLIENT[f'{company_id}_data']

        if any([start_date == 'undefined', start_date == None]):
            company = COMPANIES_COLLECTION.find_one({'_id': company_id})
            counts = company['counts']
            response = {
                'people_count': counts['people_count'],
                'journey_count': counts['journey_count'],
            }
        else:
            date_range_query = get_date_query(start_date, end_date)

            journey_count, people_count = get_counts(
                company_collection, date_range_query)

            response = {
                'people_count': people_count,
                'journey_count': journey_count,
            }

        return jsonify(response), 200
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/get-by-precent-counts", methods=['POST'])
def get_by_precent_counts():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])


        company_id = request.json.get('company_id')
        start_date = request.json.get('start_date')
        end_date = request.json.get('end_date')
        field = request.json.get('field')

        if any([start_date == 'undefined', start_date == None]):
            company = COMPANIES_COLLECTION.find_one({'_id': company_id})
            by_precent_chart = company['by_precent_chart'][field]
            response = {
                'items': by_precent_chart['item_list'],
                'counts': by_precent_chart['count_list']
            }
        else:
            date_range_query = get_date_query(start_date, end_date)

            company_collection = DB_CLIENT[f'{company_id}_data']

            item_list, count_list = get_by_precent_count(company_collection,field,date_range_query)

            response = {
                'items': item_list,
                'counts': count_list
            }

        return jsonify(response), 200

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401




@app.route("/api/get-company-popular", methods=['POST'])
def get_company_popular():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])

        user_id = payload['user_id']

        company_id = request.json.get('company_id')
        start_date = request.json.get('start_date')
        end_date = request.json.get('end_date')
        popular_type = request.json.get('type')

        company_collection = DB_CLIENT[f'{company_id}_data']
        if any([start_date == 'undefined', start_date == None]):  
            company = COMPANIES_COLLECTION.find_one({'_id': company_id})
            popular_chart = company["popular_chart"][popular_type]

            response = {
                'popular_items': popular_chart['item_list'],
                'popular_items_counts': popular_chart['count_list']
            }
        else:
            date_range_query = get_date_query(start_date, end_date)
            

            popular_items, popular_items_counts = get_most_popular(company_collection, date_range_query, popular_type)

            response = {
                'popular_items': popular_items,
                'popular_items_counts': popular_items_counts
            }

        return jsonify(response), 200
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/get-segment-filters", methods=['POST'])
def get_segment_filters():
    data = request.get_json()
    segment_id = data.get("segment_id")

    segment = SEGMENT_COLLECTION.find_one({"_id": str(segment_id)})

    return jsonify(segment["filters"])


@app.route("/api/update-company", methods=['POST'])
def update_company():
    token = request.headers.get('Authorization')
    content = request.get_json()
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
        user = USER_COLLECTION.find_one({"_id": user_id})
        if user.get('user_role') == "customer":  # Use get() instead of () for dictionaries
            raise jwt.DecodeError

        company_id = content.get('company_id')
        # Replace with the actual data to update
        new_company_data = content.get('new_company_data')

        # Use the update_one method to update the company document
        old_company = COMPANIES_COLLECTION.find_one({'_id': company_id})

        result = COMPANIES_COLLECTION.update_one(
            {"_id": company_id}, {"$set": new_company_data})

        if result:
            for user_email in new_company_data.get('attached_users', []):
                print(user_email)
                USER_COLLECTION.update_one({"user_email": user_email}, {
                                           "$addToSet": {"companies": company_id}})
            for user_email in old_company["attached_users"]:
                print(user_email)
                if user_email not in new_company_data.get('attached_users', []):
                    USER_COLLECTION.update_one({'user_email': user_email}, {
                                               '$pull': {'companies': company_id}})
            return jsonify({"message": "Company updated successfully"})
        else:
            print('no')
            return jsonify({"error": "No company updated"}), 404

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/update-segment", methods=['POST'])
def update_segment():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        data = request.get_json()
        segment_id = data.get("segment_id")
        new_segment_data = data.get("new_segment_data")
        print(new_segment_data)

        SEGMENT_COLLECTION.update_one(
            {"_id": segment_id}, {'$set': new_segment_data})

        return jsonify({"message": "Segment updated successfully"})

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/search-users", methods=['GET'])
def search_users():
    company_name = request.args.get('company-name').replace(" ", "_").lower()
    collection = DB_CLIENT[f'{company_name}_data']
    query = request.args.get('query').lower()
    regex_query = re.compile(query, re.IGNORECASE)
    instance = collection.find({'firstName': {"$regex": regex_query}})
    return json.loads(json_util.dumps(list(instance)))

def stream_chunked_documents(collection):
    # Create a chunk size for streaming
    chunk_size = 1000

    # Loop through the documents in chunks
    for chunk in collection.find({}, projection={'_id': False}, batch_size=chunk_size):
        # Convert each document to JSON string
        json_docs = [json.dumps(doc, default=json_util.default) for doc in chunk]

        # Send the JSON string as chunk of the response
        yield f'{json_docs}\n'

@app.route("/api/download-users", methods=['POST'])
def download_users():
    data = request.get_json()
    company_id = data.get('id').lower()
    search = data.get('search')
    start_date = data.get('start-date')
    end_date = data.get('end-date')
    filters = data.get('filters')

    filter_query = build_filter(filters)
    date_range_query = get_date_query(start_date, end_date)
    collection = DB_CLIENT[f'{company_id}_data']

    base_query = {
        '$and': [date_range_query, filter_query]
    }

    if search != 'false':
        regex_search = re.compile(search.lower(), re.IGNORECASE)
        base_query['fullName'] = {"$regex": regex_search}

    # Fetch user IDs to exclude
    if filters:
        if filters:
            try:
                user_ids_to_exclude = set()
                # Compile regex patterns for different types outside the loop
                regex_patterns = {
                    'contain': (lambda value: re.compile(re.escape(value), re.IGNORECASE)),
                    'start': (lambda value: re.compile(f"^{re.escape(value)}", re.IGNORECASE)),
                    'end': (lambda value: re.compile(f"{re.escape(value)}$", re.IGNORECASE)),
                }

                user_ids_to_exclude = set()

                for filter in filters:
                    if filter["id"] == 'contains':
                        exclude_array = filter["value"].get('exclude_array', [])
                        for exclude in exclude_array:
                            exclude_value = exclude['value']
                            exclude_type = exclude['type'] or 'contain'
                            exclude_regex = regex_patterns.get(exclude_type, regex_patterns['contain'])(exclude_value)
                            excluded_users = collection.find({"url": {"$regex": exclude_regex}})
                            user_ids_to_exclude.update(user['fullName'] for user in excluded_users)

                print(user_ids_to_exclude)

                if user_ids_to_exclude:
                    base_query['fullName'] = {"$nin": list(user_ids_to_exclude)}
            except KeyError:
                pass

    instance = collection.find(base_query)

    def generate():
        # Yield CSV header
        yield ','.join(DESIRED_COLUNMS_ORDER) + '\n'

        # Fetch and yield data in smaller chunks
        for doc in instance:
            yield ','.join(str(doc.get(field, '')) for field in DESIRED_COLUNMS_ORDER) + '\n'

    response = Response(generate(), mimetype='text/csv')
    response.headers['Content-Disposition'] = 'attachment; filename=export.csv'

    return response


@app.errorhandler(500)
def internal_error(error):

    return jsonify("Not Found")
