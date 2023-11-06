from datetime import datetime, timedelta
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



app = Flask(__name__)
CORS(app)
bcrypt = Bcrypt(app)


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


def get_most_popular(collection, date_range_query):
    url_list = []
    count_list = []
    pipeline = [
        {
            "$match":
            date_range_query

        },
        {
            "$group": {
                "_id": "$url",  # Group by the field you want to count
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
        url_list.append(item['_id'])
        count_list.append(item['count'])

    return url_list, count_list


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
        print(filter_params)
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
                    # column_name = include['contains_name']
                    column_name = 'url'
                    include_value = include['value']
                    include_type = include['type']
                    if include_type == 'contain':
                        or_arr.append(
                            {column_name: {"$regex": include_value, "$options": 'i'}})
                    elif include_type == 'start':
                        or_arr.append(
                            {column_name: {"$regex": f'^{include_value}', "$options": 'i'}})
                    elif include_type == 'exact':
                        or_arr.append(
                            {column_name: include_value})
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

        user = USER_COLLECTION.find_one({'_id': user_id})

        new_segment = {
            "_id": uuid.uuid4().hex,
            "segment_name": segment_name,
            "attached_company": company_id,
            "created_by": user["user_name"],
            "filters": filters
        }

        SEGMENT_COLLECTION.insert_one(new_segment)

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
                company_collection = DB_CLIENT[f'{id}_data']
                journey_count, people_count = get_counts(company_collection)

                companies.append({
                    "company_details": company,
                    "counts": {
                        "journey_count": journey_count,
                        "people_count": people_count
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
                "people_count": people_count

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
    is_segnew = False

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

        if is_segnew:
            filters = filters[1]
        if filters:
            print(f'{filter_query} qsdnfjadsbfgljsdblc')
            if len(filters) > 0:
                for filter in filters:
                    filter_key = filter["id"]
                    filter_value = filter["value"]
                    if filter_key == 'contains':
                        try:
                            exclude_array = filter_value['exclude_array']
                            or_arr = []
                            for exclude in exclude_array:
                                exclude_value = exclude['value']
                                exclude_type = exclude['type']
                                if len(exclude_value) > 0:
                                    if exclude_type == 'contain' or exclude_type == '':
                                        exclude_argg_query = {
                                                                "$regexMatch": {
                                                                    "input": "$url",
                                                                    "regex": re.escape(exclude_value),
                                                                    "options": 'i'
                                                                }
                                                            }
                                    elif exclude_type == 'start':
                                        exclude_argg_query = {
                                                                "$regexMatch": {
                                                                    "input": "$url",
                                                                    "regex": f'^{re.escape(exclude_value)}',
                                                                    "options": 'i'
                                                                }
                                                            }
                                    elif exclude_type == 'exact':
                                        exclude_argg_query = {
                                                                'url': exclude_value
                                                            }
                                    pipeline = [
                                        {
                                            '$skip': skip
                                        },
                                        {
                                            "$group": {
                                                "_id": "$fullName",
                                                "contains_excluded_url": {
                                                    "$max": {
                                                        "$cond": [
                                                            exclude_argg_query,
                                                            1,
                                                            0
                                                        ]
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "$match": {
                                                "contains_excluded_url": 1
                                            }
                                        },
                                        {
                                            '$limit': ITEMS_PER_PAGE
                                        },
                                    ]
                                    user_ids = [user['_id'] for user in collection.aggregate(pipeline)]
                                    or_arr.append({"fullName": {"$nin": user_ids}})
                            base_query['$and'].append({"$or": or_arr})
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
    

    response_data = {
        'users': json.loads(json_util.dumps(list(instance))),
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


@app.route("/api/get-company-popular", methods=['POST'])
def get_company_popular():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])

        user_id = payload['user_id']

        company_id = request.json.get('company_id')
        start_date = request.json.get('start_date')
        end_date = request.json.get('end_date')

        company_collection = DB_CLIENT[f'{company_id}_data']

        date_range_query = get_date_query(start_date, end_date)

        popular_urls, urls_counts = get_most_popular(
            company_collection, date_range_query)

        response = {
            'popular_urls': popular_urls,
            'popular_urls_counts': urls_counts
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


@app.route("/api/download-users", methods=['POST'])
def download_users():
    data = request.get_json()
    company_id = data.get('id').lower()

    collection = DB_CLIENT[f'{company_id}_data']

    with open('output.csv', "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=DESIRED_COLUNMS_ORDER)
        writer.writeheader()

        for document in collection.find():
            row = {field: document.get(field, "")
                   for field in DESIRED_COLUNMS_ORDER}
            writer.writerow(row)

    return send_from_directory('', 'output.csv', as_attachment=True)

    # data = request.get_json()

    # company_id = data.get('id').lower()
    # search = data.get('search')
    # start_date = data.get('start-date')
    # end_date = data.get('end-date')
    # filters = data.get('filter')

    # filter_query = build_filter(filters)

    # collection = DB_CLIENT[f'{company_id}_data']

    # date_range_query = get_date_query(start_date, end_date)

    # # if len(search) > 0:
    # #     regex_search = re.compile(search.lower(), re.IGNORECASE)
    # #     instance = collection.find({
    # #         '$and': [
    # #             {'fullName': {"$regex": regex_search}},
    # #             date_range_query,filter_query
    # #         ]
    # #     })

    # # else:
    # instance = collection.find({'$and': [
    #     # date_range_query,
    #     filter_query
    # ]})

    # print(list(instance))
    # print('dasd')

    # def generate():
    #     # Yield CSV header
    #     yield ','.join(DESIRED_COLUNMS_ORDER) + '\n'

    #     # Fetch and yield data in smaller chunks
    #     for doc in instance:
    #         yield ','.join(str(doc.get(field, '')) for field in DESIRED_COLUNMS_ORDER) + '\n'

    # response = Response(generate(), mimetype='text/csv')
    # response.headers['Content-Disposition'] = 'attachment; filename=export.csv'

    # return response


@app.errorhandler(500)
def internal_error(error):

    return jsonify("Not Found")


if __name__ == '__main__':
    app.run(port='8080')
