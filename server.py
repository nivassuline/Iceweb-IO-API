from datetime import datetime
from flask import Flask, request, jsonify, make_response, send_file
from flask_bcrypt import Bcrypt
from urllib.parse import urlparse
from pymongo import MongoClient
from bson import json_util
from datetime import datetime
import pandas as pd
from flask_cors import CORS
import random
import json
import uuid
import jwt
import re
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text,types
import os
from apscheduler.schedulers.background import BackgroundScheduler
from integrations import *
from azure_functions import *
from postgres_functions import *
from app_functions import *
from update_functions import *
from get_functions import *
from build_functions import *
import logging


app = Flask(__name__)
app.logger.setLevel(logging.INFO)
CORS(app)
bcrypt = Bcrypt(app)
scheduler = BackgroundScheduler()
scheduler.start()

POSTGRES_CONN_STRING = 'host=c-icewebio-postgres.tfgc42d4iqjg72.postgres.cosmos.azure.com port=5432 dbname=citus user=citus password=iceWeb1234 sslmode=require'
POSTGRES_DATABASE_URI = "postgresql://citus:iceWeb1234@c-icewebio-postgres.tfgc42d4iqjg72.postgres.cosmos.azure.com:5432/citus"
POSTGRES_CONNECTION = psycopg2.pool.SimpleConnectionPool(
    1, 20, 'host=c-icewebio-postgres.tfgc42d4iqjg72.postgres.cosmos.azure.com port=5432 dbname=citus user=citus password=iceWeb1234 sslmode=require')
DB_CONNECTOR = MongoClient(
    "mongodb://icwebio:yJtwZocIwp8tZ4CQGAnRVvBxCv2i3bGIDHa3Za7w6sP3ww5I5Bgm8e1OpSSMbvdLIpEh7KDeWX4WACDbtUkLGw==@icwebio.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@icwebio@")

DB_CLIENT = DB_CONNECTOR['main']
ADMIN_DB = DB_CONNECTOR
COMPANIES_COLLECTION = DB_CLIENT['companies']
USER_COLLECTION = DB_CLIENT['users']
ROLE_COLLECTION = DB_CLIENT['roles']
SEGMENT_COLLECTION = DB_CLIENT['segments']
INTEGRATION_COLLECTION = DB_CLIENT['integrations']
LOGS_COLLECTION = DB_CLIENT['logs']
SECRET_KEY = 'iceweb123456789'
AZURE_ACCOUNT_URL = "https://icewebstorage.blob.core.windows.net"
AZURE_CONNECTION_BLOB_STRING = "zQNgBDFROUur92AMQIDwSoIm3Fswg4rCmjHniH3wvIMLnP8ewXdBISHa1yCxG/obFJHufoAlo/NZ+ASt5bMcvg=="
AZURE_CONTAINER_NAME = "icewebio"
ITEMS_PER_PAGE = 13

ENGINE = create_engine(POSTGRES_DATABASE_URI, pool_size=20, max_overflow=10)
DB_CLIENT.command("enableSharding", "main")



DESIRED_COLUNMS_ORDER = ["date", "id", "hour", "fullName", "firstName", "lastName", "url", "facebook", "linkedIn", "twitter", "email", "optIn", "optInDate", "optInIp", "optInUrl", "pixelFirstHitDate", "pixelLastHitDate", "bebacks", "phone", "dnc", "age", "gender", "maritalStatus", "address", "city", "state", "zip", "householdIncome", "netWorth", "incomeLevels", "peopleInHousehold", "adultsInHousehold", "childrenInHousehold", "veteransInHousehold", "education", "creditRange", "ethnicGroup", "generation", "homeOwner", "occupationDetail", "politicalParty", "religion", "childrenBetweenAges0_3", "childrenBetweenAges4_6", "childrenBetweenAges7_9",
                         "childrenBetweenAges10_12", "childrenBetweenAges13_18", "behaviors", "childrenAgeRanges", "interests", "ownsAmexCard", "ownsBankCard", "dwellingType", "homeHeatType", "homePrice", "homePurchasedYearsAgo", "homeValue", "householdNetWorth", "language", "mortgageAge", "mortgageAmount", "mortgageLoanType", "mortgageRefinanceAge", "mortgageRefinanceAmount", "mortgageRefinanceType", "isMultilingual", "newCreditOfferedHousehold", "numberOfVehiclesInHousehold", "ownsInvestment", "ownsPremiumAmexCard", "ownsPremiumCard", "ownsStocksAndBonds", "personality", "isPoliticalContributor", "isVoter", "premiumIncomeHousehold", "urbanicity", "maid", "maidOs"]

COLUMNS_ORDER = ['date', 'date_added', 'url', 'full_name', 'first_name', 'last_name', 'facebook', 'linked_in', 'twitter', 'email', 'opt_in', 'opt_in_date', 'opt_in_ip', 'opt_in_url', 'pixel_first_hit_date', 'pixel_last_hit_date', 'bebacks', 'phone', 'dnc', 'age', 'gender', 'marital_status', 'address', 'city', 'state', 'zip', 'household_income', 'net_worth', 'income_levels', 'people_in_household', 'adults_in_household', 'children_in_household', 'veterans_in_household', 'education', 'credit_range', 'ethnic_group', 'generation', 'home_owner', 'occupation_detail', 'political_party', 'religion', 'children_between_ages0_3', 'children_between_ages4_6', 'children_between_ages7_9', 'children_between_ages10_12',
                 'children_between_ages13_18', 'behaviors', 'children_age_ranges', 'interests', 'owns_amex_card', 'owns_bank_card', 'dwelling_type', 'home_heat_type', 'home_price', 'home_purchased_years_ago', 'home_value', 'household_net_worth', 'language', 'mortgage_age', 'mortgage_amount', 'mortgage_loan_type', 'mortgage_refinance_age', 'mortgage_refinance_amount', 'mortgage_refinance_type', 'is_multilingual', 'new_credit_offered_household', 'number_of_vehicles_in_household', 'owns_investment', 'owns_premium_amex_card', 'owns_premium_card', 'owns_stocks_and_bonds', 'personality', 'is_political_contributor', 'is_voter', 'premium_income_household', 'urbanicity', 'maid', 'maid_os', 'hour']

desired_columns_order_journey = ['date', 'date_added', 'hour', "url", "full_name", "firstName", "lastName", "facebook", "linkedIn", "twitter", "email", "optIn", "optInDate", "optInIp", "optInUrl", "pixelFirstHitDate", "pixelLastHitDate", "bebacks", "phone", "dnc", "age", "gender", "maritalStatus", "address", "city", "state", "zip", "householdIncome", "netWorth", "incomeLevels", "peopleInHousehold", "adultsInHousehold", "childrenInHousehold", "veteransInHousehold", "education", "creditRange", "ethnicGroup", "generation", "homeOwner", "occupationDetail", "politicalParty", "religion", "childrenBetweenAges0_3", "childrenBetweenAges4_6", "childrenBetweenAges7_9",
                                 "childrenBetweenAges10_12", "childrenBetweenAges13_18", "behaviors", "childrenAgeRanges", "interests", "ownsAmexCard", "ownsBankCard", "dwellingType", "homeHeatType", "homePrice", "homePurchasedYearsAgo", "homeValue", "householdNetWorth", "language", "mortgageAge", "mortgageAmount", "mortgageLoanType", "mortgageRefinanceAge", "mortgageRefinanceAmount", "mortgageRefinanceType", "isMultilingual", "newCreditOfferedHousehold", "numberOfVehiclesInHousehold", "ownsInvestment", "ownsPremiumAmexCard", "ownsPremiumCard", "ownsStocksAndBonds", "personality", "isPoliticalContributor", "isVoter", "premiumIncomeHousehold", "urbanicity", "maid", "maidOs"]

JOURNEY_FILE_COLUMNS_ORDER = ['date', 'date_added', 'hour', 'full_name', 'url', 'facebook', 'linked_in', 'twitter', 'email', 'opt_in', 'opt_in_date', 'opt_in_ip', 'opt_in_url', 'pixel_first_hit_date', 'pixel_last_hit_date', 'bebacks', 'phone', 'dnc', 'age', 'gender', 'marital_status', 'address', 'city', 'state', 'zip', 'household_income', 'net_worth', 'income_levels', 'people_in_household', 'adults_in_household', 'children_in_household', 'veterans_in_household', 'education', 'credit_range', 'ethnic_group', 'generation', 'home_owner', 'occupation_detail', 'political_party', 'religion', 'children_between_ages0_3', 'children_between_ages4_6', 'children_between_ages7_9',
                              'children_between_ages10_12', 'children_between_ages13_18', 'behaviors', 'children_age_ranges', 'interests', 'owns_amex_card', 'owns_bank_card', 'dwelling_type', 'home_heat_type', 'home_price', 'home_purchased_years_ago', 'home_value', 'household_net_worth', 'language', 'mortgage_age', 'mortgage_amount', 'mortgage_loan_type', 'mortgage_refinance_age', 'mortgage_refinance_amount', 'mortgage_refinance_type', 'is_multilingual', 'new_credit_offered_household', 'number_of_vehicles_in_household', 'owns_investment', 'owns_premium_amex_card', 'owns_premium_card', 'owns_stocks_and_bonds', 'personality', 'is_political_contributor', 'is_voter', 'premium_income_household', 'urbanicity', 'maid', 'maid_os']

USERS_FILE_COLUMNS_ORDER = ['date', 'date_added', 'full_name', 'email', 'facebook', 'linked_in', 'twitter', 'opt_in', 'opt_in_date', 'opt_in_ip', 'opt_in_url', 'pixel_first_hit_date', 'pixel_last_hit_date', 'bebacks', 'phone', 'dnc', 'age', 'gender', 'marital_status', 'address', 'city', 'state', 'zip', 'household_income', 'net_worth', 'income_levels', 'people_in_household', 'adults_in_household', 'children_in_household', 'veterans_in_household', 'education', 'credit_range', 'ethnic_group', 'generation', 'home_owner', 'occupation_detail', 'political_party', 'religion', 'children_between_ages0_3', 'children_between_ages4_6', 'children_between_ages7_9',
                            'children_between_ages10_12', 'children_between_ages13_18', 'behaviors', 'children_age_ranges', 'interests', 'owns_amex_card', 'owns_bank_card', 'dwelling_type', 'home_heat_type', 'home_price', 'home_purchased_years_ago', 'home_value', 'household_net_worth', 'language', 'mortgage_age', 'mortgage_amount', 'mortgage_loan_type', 'mortgage_refinance_age', 'mortgage_refinance_amount', 'mortgage_refinance_type', 'is_multilingual', 'new_credit_offered_household', 'number_of_vehicles_in_household', 'owns_investment', 'owns_premium_amex_card', 'owns_premium_card', 'owns_stocks_and_bonds', 'personality', 'is_political_contributor', 'is_voter', 'premium_income_household', 'urbanicity']


@app.route("/api/test", methods=['GET'])
def tes():
    create_df_file_from_db(
        AZURE_ACCOUNT_URL,
        AZURE_CONNECTION_BLOB_STRING,
        USERS_FILE_COLUMNS_ORDER,
        JOURNEY_FILE_COLUMNS_ORDER,
        ENGINE,
        AZURE_CONTAINER_NAME,
        'xs249'
    )

    return jsonify('done!')


@app.route("/api/data-changed", methods=['POST'])
def data_changed():
    data = request.get_json()
    company_id = data.get('company_id')

    create_df_file_from_db(
        AZURE_ACCOUNT_URL,
        AZURE_CONNECTION_BLOB_STRING,
        USERS_FILE_COLUMNS_ORDER,
        JOURNEY_FILE_COLUMNS_ORDER,
        ENGINE,
        AZURE_CONTAINER_NAME,
        company_id
    )
    update_company_counts(ENGINE, company_id,
                          COMPANIES_COLLECTION=COMPANIES_COLLECTION)
    update_popular_chart(ENGINE, company_id,
                         COMPANIES_COLLECTION=COMPANIES_COLLECTION)
    update_by_percent(ENGINE, company_id,
                      COMPANIES_COLLECTION=COMPANIES_COLLECTION)
    update_average_credit_score(ENGINE,company_id,COMPANIES_COLLECTION=COMPANIES_COLLECTION)

    segments = SEGMENT_COLLECTION.find({'attached_company': company_id})
    for segment in segments:
        filter_query = build_filter(company_id, segment['filters'])
        create_df_file_from_db(
            AZURE_ACCOUNT_URL,
            AZURE_CONNECTION_BLOB_STRING,
            USERS_FILE_COLUMNS_ORDER,
            JOURNEY_FILE_COLUMNS_ORDER,
            ENGINE,
            AZURE_CONTAINER_NAME,
            company_id,
            segment["_id"],
            segment['filters']
        )
        update_company_counts(ENGINE, company_id, SEGMENT_COLLECTION=SEGMENT_COLLECTION,
                              segment_id=segment['_id'], filter_query=filter_query)
        update_popular_chart(ENGINE, company_id, SEGMENT_COLLECTION=SEGMENT_COLLECTION,
                             segment_id=segment['_id'], filter_query=filter_query)
        update_by_percent(ENGINE, company_id, SEGMENT_COLLECTION=SEGMENT_COLLECTION,
                          segment_id=segment['_id'], filter_query=filter_query)
        update_average_credit_score(ENGINE,company_id,SEGMENT_COLLECTION=SEGMENT_COLLECTION,segment_id=segment["_id"],filter_query=filter_query)
    for user in USER_COLLECTION.find({'companies': company_id}):
        if user["integrations"]:
            for integration in user["integrations"]:
                update_integration(
                    ENGINE,
                    company_id,
                    integration["api_key"],
                    integration["integration_name"],
                    integration["site_id"],
                    integration["list_id"],
                    integration["client_secret"],
                    integration["public_id"]
                )

    return jsonify('done!')


@app.route("/api/login", methods=['POST'])
def login():
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    user = USER_COLLECTION.find_one({'user_email': email})

    if user and bcrypt.check_password_hash(user['user_password'], password):
        token = generate_jwt_token(user["_id"], user["user_role"], SECRET_KEY)
        # Password is correct
        # You can generate a token here and exclude it in the response for future authenticated requests
        response = {
            'message': 'Login successful',
            # Convert ObjectId to string for JSON serialization
            'user_id': user['_id'],
            'token': token
        }
        add_log(LOGS_COLLECTION,request.endpoint,{"email" : email, "password": user["user_password"]},response,user_id=user["_id"])
        return jsonify(response), 200
    else:
        response = {
            'message': 'Login Failed',
            'user_id': user['_id'],
        }
        add_log(LOGS_COLLECTION,request.endpoint,{"email" : email, "password": user["user_password"]},response,user_id=user["_id"])
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
        'companies': companies_list,
        'integrations': []
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
        'attached_users': attached_users,
    }

    USER_COLLECTION.update_many({"user_role": "admin"}, {
                                "$push": {"companies": company_id.lower()}})
    COMPANIES_COLLECTION.insert_one(new_company)

    create_postgres_table(company_id.lower(), COLUMNS_ORDER, ENGINE)

    update_company_counts(company_id.lower())
    update_popular_chart(company_id.lower())
    update_by_percent(company_id.lower())

    return jsonify({'message': 'Registration successful'}), 201  # Created



@app.route("/api/add-integration", methods=['POST'])
def add_integration():
    token = request.headers.get('Authorization')
    try:
        data = request.get_json()
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
        company_id = data.get('company_id')
        integration_name = data.get('integration_name')
        api_key = data.get('api_key')
        site_id = data.get('site_id')
        list_id = data.get('list_id')
        client_secret = data.get('client_secret')
        public_id = data.get('public_id')
        filters = data.get('filters')
        segment_id = data.get('segment_id')

        print(client_secret)
        print(public_id)

        user = USER_COLLECTION.find_one({'_id': user_id})

        integration_id = uuid.uuid4().hex

        new_integration = {
            "id": integration_id,
            "integration_name": integration_name,
            "attached_company": company_id,
            "api_key": api_key,
            "site_id": site_id,
            "list_id": list_id,
            "client_secret": client_secret,
            "public_id": public_id,
            "sync_started" : 0,
            "sync_progress": 0,
        }

        if segment_id:
            SEGMENT_COLLECTION.update_one(
                {'_id': segment_id}, {'$push': {'integrations': new_integration}})
        else:
            USER_COLLECTION.update_one(
                {'_id': user_id}, {'$push': {'integrations': new_integration}})

        filter_query = build_filter(company_id, filters)

        # update_integration(company_id,api_key,integration_name,site_id,list_id)

        job = scheduler.add_job(func=update_integration, misfire_grace_time=15*60,
                                args=[ENGINE,company_id, api_key, integration_name, site_id, list_id, filter_query])

        job.modify(next_run_time=datetime.now())

        return jsonify('Done!')
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        return jsonify({"error": "Invalid token"}), 401


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

        segment_id = uuid.uuid4().hex

        new_segment = {
            "_id": segment_id,
            "segment_name": segment_name,
            "attached_company": company_id,
            "created_by": user["user_name"],
            "filters": filters,
            "integrations": []
        }

        SEGMENT_COLLECTION.insert_one(new_segment)

        job = scheduler.add_job(func=create_df_file_from_db, misfire_grace_time=15*60,
                                args=[AZURE_ACCOUNT_URL,
                                      AZURE_CONNECTION_BLOB_STRING,
                                      USERS_FILE_COLUMNS_ORDER,
                                      JOURNEY_FILE_COLUMNS_ORDER,
                                      ENGINE,
                                      AZURE_CONTAINER_NAME,
                                      company_id,
                                        segment_id,
                                      filters])

        job.modify(next_run_time=datetime.now())

        filter_query = build_filter(company_id, filters)

        update_company_counts(ENGINE,company_id,COMPANIES_COLLECTION,SEGMENT_COLLECTION,segment_id,filter_query)
        update_popular_chart(ENGINE,company_id,SEGMENT_COLLECTION,COMPANIES_COLLECTION,segment_id,filter_query)
        update_by_percent(ENGINE,company_id,COMPANIES_COLLECTION,SEGMENT_COLLECTION,segment_id,filter_query)

        return jsonify('Done!')
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/delete-user", methods=['DELETE'])
def delete_user():
    user_id = request.args.get('user_id')

    user = USER_COLLECTION.find_one({'_id': user_id})
    USER_COLLECTION.delete_one({'_id': user_id})
    COMPANIES_COLLECTION.update_many({'attached_users': {'$regex': user['user_email']}}, {
                                     '$pull': {'attached_users': user['user_email']}})

    return jsonify('done!')



@app.route("/api/delete-company", methods=['DELETE'])
def delete_company():
    company_id = request.args.get('company_id')

    COMPANIES_COLLECTION.delete_one({'_id': company_id})
    USER_COLLECTION.update_many({'companies': {'$regex': company_id}}, {
                                '$pull': {'companies': company_id}})

    delete_postgres_table(company_id, ENGINE)

    return jsonify("done!")


@app.route("/api/delete-segment", methods=['DELETE'])
def delete_segment():
    segment_id = request.args.get('segment_id')

    segment = SEGMENT_COLLECTION.find_one_and_delete({'_id': segment_id})

    # blob_name = f'{segment["attached_company"]}/{segment_id}'

    delete_from_azure_blob(AZURE_ACCOUNT_URL, AZURE_CONNECTION_BLOB_STRING,
                           AZURE_CONTAINER_NAME, segment["attached_company"], segment_id)

    return jsonify("done!")


@app.route("/api/import", methods=['POST'])
def import_data():
    company_id = request.args.get('id')
    print(company_id)
    path = '/Users/neevassouline/Desktop/Coding Projects/IcewebIO/data1.csv'

    df = pd.read_csv(path)
    df.fillna('-', inplace=True)

    df["date"] = pd.to_datetime(df["date"])
    df['date_added'] = datetime.now().date()
    df['date_added'] = df['date_added'].astype(str)
    df['hour'] = df['date'].dt.strftime('%H:%M:%S')
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df['full_name'] = df['firstName'] + ' ' + df['lastName']
    df['url'] = df['url'].apply(lambda x: x.replace(
        f'{urlparse(x).scheme}://{urlparse(x).netloc}', ''))
    

    df_filtered = df[desired_columns_order_journey]
    df_filtered = df_filtered.rename(columns=lambda x: camel_to_snake(x))
    df_filtered.to_sql(company_id, ENGINE, if_exists='append', index=False)

    return jsonify('Done!')

@app.route("/api/import-pearldiver", methods=['POST'])
def import_pearldiver():
    company_id = request.args.get('id')
    print(company_id)
    path = '/Users/neevassouline/Desktop/Coding Projects/IcewebIO/data.csv'

    mapping = {
    'LastActivityDate': 'date',
    'FirstName': 'firstName',
    'LastName': 'lastName',
    'Email': 'email',
    'Gender': 'gender',
    'AgeRange': 'age',
    'LinkedIn': 'linkedIn',
    'PersonalCity': 'city',
    'PersonalState': 'state',
    'PersonalZIP': 'zip',
    'MobilePhone1': 'phone'
    # Add more mappings as needed
}

    df = pd.read_csv(path)

    df = df[list(mapping.keys())].rename(columns=mapping)

    df['hour'] = "00:00:00"  # Add default value or update based on your data
    df['url'] = "/"   # Add default value or update based on your data
    df['bebacks'] = 0

    for column in desired_columns_order_journey:
        if column not in ['hour', 'url','date', 'firstName','lastName', 'email','gender','age','linkedIn','city','state','zip','phone','bebacks']:
            df[column] = '-'

    df['date_added'] = datetime.now().date()
    df['full_name'] = df['firstName'] + ' ' + df['lastName']
    df['full_name'] = df['full_name'].fillna(df['email'].str.split('@').str[0])
    df['zip'] = df['zip'].fillna(0)
    # df['zip'] = pd.to_numeric(df['zip'])
    df['gender'] = df['gender'].replace({'M': 'Male', 'F': 'Female', 'U' : '-'})
    df['age'] = df['age'].replace({'18-24': '20', '25-34': '30', '35-44': '40', '45-54': '50', '55-64': '60', '65 and older': '70'})


    df.fillna('-', inplace=True)

    df_filtered = df[desired_columns_order_journey]
    df_filtered = df_filtered.rename(columns=lambda x: camel_to_snake(x))


    # column_data_types = {col: types.String() for col in df_filtered.columns}
    # column_data_types.update({col: types.BigInteger() for col in ['bebacks', 'zip']})



    df_filtered.to_sql(company_id, ENGINE, if_exists='append', index=False)
    return jsonify('Done!')

@app.route("/api/get-company-list", methods=['GET'])
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
                try:
                    counts = company['counts']
                    companies.append({
                        "company_details": company,
                        "counts": {
                            "journey_count": counts['journey_count'],
                            "people_count": counts['people_count']
                        }
                    })
                except KeyError:
                    companies.append({
                        "company_details": company,
                        "counts": {
                            "journey_count": 0,
                            "people_count": 0
                        }
                    })

        return jsonify(companies), 200
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/get-user-list", methods=['GET'])
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


@app.route("/api/get-user-integrations", methods=['POST'])
def get_user_integrations():
    token = request.headers.get('Authorization')
    try:
        data = request.get_json()
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
        segment_id = data.get('segment_id')

        if segment_id:
            segment = SEGMENT_COLLECTION.find_one({'_id': segment_id})
            return jsonify(segment['integrations'])
        else:
            user = USER_COLLECTION.find_one({'_id': user_id})
            return jsonify(user['integrations'])

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
            filter_query = build_filter(company_id, segment["filters"])
            people_count = get_counts(ENGINE, company_id, build_date_query(
                None, None), filter_query=filter_query)[1]
            segment_list.append({
                "segment_id": segment["_id"],
                "segment_name": segment["segment_name"],
                "created_by": segment["created_by"],
                "people_count": people_count,
                "filters": segment["filters"],
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

def get_total_rows(query):
    count_query = text(f"SELECT COUNT(*) FROM ({query}) as subquery")
    with ENGINE.connect() as connection:
        total_rows = connection.execute(count_query).scalar()
    return total_rows


@app.route("/api/get-data-rows-count", methods=['POST'])
def get_data_rows_count():
    data = request.get_json()
    company_id = data.get('id').lower()
    start_date = data.get('start-date')
    end_date = data.get('end-date')
    page = data.get('page')
    column_filters = data.get('column_filter')
    filters = data.get('filters')
    data_type = data.get('data_type')

    try:
        if len(filters) > 0:
            filter_query = build_filter(company_id, filters, column_filters)

        elif len(filters) == 0:
            return jsonify('ignore')
    except TypeError:
        try:
            if filters[0] == 'segnew':
                filter_query = build_filter(
                    company_id, filter_params=filters[1])
        except IndexError:
            filter_query = build_filter(
                company_id, filter_params=None, column_filters=column_filters)
        except TypeError:
            filter_query = build_filter(
                company_id, filter_params=None, column_filters=column_filters)

    skip = page * ITEMS_PER_PAGE

    print(filter_query)

    date_range_query = build_date_query(start_date, end_date)

    if filter_query is None:
        if data_type == 'users':
            query = text(
                f"SELECT COUNT(DISTINCT full_name) FROM public.{company_id} WHERE ({date_range_query});")  
        else:
            query = text(
                f"SELECT COUNT(*) FROM {company_id} WHERE ({date_range_query});")

    else:
        if data_type == 'users':
            # Final query
            query = text(f"""
                SELECT COUNT(DISTINCT full_name)
                FROM public.{company_id}
                WHERE ({date_range_query} AND {filter_query})
            """)
        else:
            query = text(f"""
                SELECT COUNT(*)
                FROM public.{company_id}
                WHERE ({date_range_query} AND {filter_query})
            """)


    with ENGINE.connect() as connection:
        total_rows = connection.execute(query).scalar()
        
    # Return data as JSON
    return jsonify(total_rows)


@app.route("/api/get-data", methods=['POST'])
def get_data():
    data = request.get_json()
    company_id = data.get('id').lower()
    start_date = data.get('start-date')
    end_date = data.get('end-date')
    page = data.get('page')
    column_filters = data.get('column_filter')
    filters = data.get('filters')
    data_type = data.get('data_type')
    full_data = data.get('full_data')

    try:
        if len(filters) > 0:
            filter_query = build_filter(company_id, filters, column_filters)

        elif len(filters) == 0:
            return jsonify('ignore')
    except TypeError:
        try:
            if filters[0] == 'segnew':
                filter_query = build_filter(
                    company_id, filter_params=filters[1])
        except IndexError:
            filter_query = build_filter(
                company_id, filter_params=None, column_filters=column_filters)
        except TypeError:
            filter_query = build_filter(
                company_id, filter_params=None, column_filters=column_filters)

    skip = page * ITEMS_PER_PAGE

    date_range_query = build_date_query(start_date, end_date)

    if filter_query is None:
        if data_type == 'users':
            query = text(
                f"SELECT DISTINCT ON (full_name) * FROM {company_id} WHERE ({date_range_query}) LIMIT {ITEMS_PER_PAGE} OFFSET {skip};")
        else:
            query = text(
                f"SELECT  * FROM {company_id} WHERE ({date_range_query}) LIMIT {ITEMS_PER_PAGE} OFFSET {skip};")

    else:
        if data_type == 'users':
            # Final query
            query = text(f"""
                SELECT DISTINCT ON (full_name) *
                FROM public.{company_id}
                WHERE ({date_range_query} AND {filter_query})
                LIMIT {ITEMS_PER_PAGE} OFFSET {skip}
            """)
        else:
            query = text(f"""
                SELECT *
                FROM public.{company_id}
                WHERE ({date_range_query} AND {filter_query})
                LIMIT {ITEMS_PER_PAGE} OFFSET {skip}
            """)


    with ENGINE.connect() as connection:
        data = connection.execute(query)
        colum_list = data.keys()
        result = [dict(zip(colum_list, row)) for row in data]

    # Return data as JSON
    return jsonify(result)


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


@app.route("/api/get-user-journey", methods=['POST'])
def get_user_journey():
    data = request.get_json()
    company_id = data.get('id').lower()
    user_name = data.get('user_name')

    query = text(f"""
        SELECT 
            date,
            full_name,
            url,
            opt_in_ip,
            hour,
            -- Extract parameters and remove them from the url
            REGEXP_REPLACE(url, '\\?.*', '') AS url_without_params,
            -- Extract parameters into a list
            STRING_TO_ARRAY(REGEXP_REPLACE(url, '.*\\?', ''), '&') AS parameters_list
        FROM {company_id}
        WHERE full_name = '{user_name}'
        ORDER BY 
            TO_DATE(date, 'YYYY-MM-DD') ASC,
            TO_TIMESTAMP(hour, 'HH24:MI:SS') ASC;
    """)

    with ENGINE.connect() as connection:
        data = connection.execute(query)

    # Organize the data by date
    organized_data = {}
    for row in data:
        date = row[0]  # Assuming 'date' is the name of your date column
        document = {
            'full_name': row[1],
            'url': row[2],
            'opt_in_ip': row[3],
            'hour': row[4],
            'url_without_params': row[5],
            'parameters_list': row[6]
        }

        # Append the document to the list for the corresponding date
        if date in organized_data:
            organized_data[date].append(document)
        else:
            organized_data[date] = [document]

    # Convert the organized data to a list for jsonify
    result = [{'date': date, 'documents': documents}
              for date, documents in organized_data.items()]

    return jsonify(result)


@app.route("/api/get-company-counts", methods=['POST'])
def get_company_counts():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])

        user_id = payload['user_id']

        company_id = request.json.get('company_id')
        start_date = request.json.get('start_date')
        end_date = request.json.get('end_date')
        filters = request.json.get('filters')
        segment_id = request.json.get('segment_id')

        print(f'com {company_id}')
        filter_query = build_filter(company_id, filters)

        try:
            if any([start_date == 'undefined', start_date == None]):
                if segment_id:
                    company = SEGMENT_COLLECTION.find_one({'_id': segment_id})
                else:
                    company = COMPANIES_COLLECTION.find_one(
                        {'_id': company_id})
                counts = company['counts']
                response = {
                    'people_count': counts['people_count'],
                    'journey_count': counts['journey_count'],
                }
            else:
                date_range_query = build_date_query(start_date, end_date)

                journey_count, people_count = get_counts(
                    ENGINE,company_id, date_range_query, filter_query)

                response = {
                    'people_count': people_count,
                    'journey_count': journey_count,
                }
        except KeyError:
            response = {
                'people_count': 0,
                'journey_count': 0,
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
        filters = request.json.get('filters')
        segment_id = request.json.get('segment_id')

        filter_query = build_filter(company_id, filters)

        try:
            if any([start_date == 'undefined', start_date == None]):
                if segment_id:
                    company = SEGMENT_COLLECTION.find_one({'_id': segment_id})
                else:
                    company = COMPANIES_COLLECTION.find_one(
                        {'_id': company_id})
                print('after')
                by_precent_chart = company['by_percent_chart'][field]
                response = {
                    'items': by_precent_chart['item_list'],
                    'counts': by_precent_chart['count_list']
                }
            else:
                date_range_query = build_date_query(start_date, end_date)

                item_list, count_list = get_by_precent_count(
                    ENGINE, company_id, field, date_range_query, filter_query)

                response = {
                    'items': item_list,
                    'counts': count_list
                }
        except KeyError as e:
            response = {
                'items': [],
                'counts': []
            }

        return jsonify(response), 200

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/get-average", methods=['POST'])
def get_average_app():
    company_id = request.json.get('company_id')
    segment_id = request.json.get('segment_id')
    average_type = request.json.get('average_type')
    filters = request.json.get('filters')
    start_date = request.json.get('start_date')
    end_date = request.json.get('end_date')

    filters_query = build_filter(company_id,filters)
    date_query = build_date_query(start_date,end_date)

    try:
        if any([start_date == 'undefined', start_date == None]):
            if segment_id:
                company = SEGMENT_COLLECTION.find_one({'_id': segment_id})
            else:
                company = COMPANIES_COLLECTION.find_one(
                    {'_id': company_id})
            value = company["average_credit_score"]

        else:
            value = get_average(ENGINE,average_type,company_id,date_query,filters_query)

    except KeyError:
            value = 0

    return jsonify(value)





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
        state = request.json.get('state')
        filters = request.json.get('filters')
        segment_id = request.json.get('segment_id')

        filter_query = build_filter(company_id, filters)

        try:
            if any([start_date == 'undefined', start_date == None]) and popular_type != 'city':
                if segment_id:
                    print(segment_id)
                    company = SEGMENT_COLLECTION.find_one({'_id': segment_id})
                else:
                    company = COMPANIES_COLLECTION.find_one(
                        {'_id': company_id})
                popular_chart = company["popular_chart"][popular_type]

                response = {
                    'popular_items': popular_chart['item_list'],
                    'popular_items_counts': popular_chart['count_list']
                }
            else:
                date_range_query = build_date_query(start_date, end_date)

                popular_items, popular_items_counts = get_most_popular(
                    ENGINE, company_id, date_range_query, popular_type, state, filter_query)

                response = {
                    'popular_items': popular_items,
                    'popular_items_counts': popular_items_counts
                }
        except KeyError:
            response = {
                'popular_items': [],
                'popular_items_counts': []
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

@app.route("/api/get-company-utm", methods=['POST'])
def get_company_utm():
    data = request.get_json()
    company_id = data.get('company_id')
    segment_id = data.get('segment_id')
    selection = data.get('selection')
    filters = data.get('filters')

    print(f'asdasd {filters}')

    filters = build_filter(company_id,filters)

    if filters is not None:
        if selection == 'source':
            query = text(f"""
                    SELECT DISTINCT
                        regexp_replace(
                            CASE
                                WHEN url LIKE '%utm_source%' THEN regexp_substr(url, 'utm_source=([^&]+)')
                                WHEN url LIKE '%src%' THEN regexp_substr(url, 'src=([^&]+)')
                            END,
                            'utm_source=|src=',
                            ''
                        ) AS parameter_value
                    FROM
                        {company_id}
                    WHERE
                        (url LIKE '%utm_source%'
                        OR url LIKE '%src%')
                        AND 
                        ({filters});
                            """)
        elif selection == 'medium':
            query = text(f"""
                    SELECT DISTINCT
                        regexp_replace(regexp_substr(url, 'utm_medium=([^&]+)'), 'utm_medium=', '') AS utm_medium
                    FROM
                        {company_id}
                    WHERE
                        (url LIKE '%utm_medium%')
                        AND
                        ({filters});
                            """)
        elif selection == 'name':
            query = text(f"""
                    SELECT DISTINCT
                        regexp_replace(regexp_substr(url, 'utm_campaign=([^&]+)'), 'utm_campaign=', '') AS utm_campaign
                    FROM
                        {company_id}
                    WHERE
                        (url LIKE '%utm_campaign%')
                        AND
                        ({filters});
                            """)
    else:
        if selection == 'source':
            query = text(f"""
                    SELECT DISTINCT
                        regexp_replace(
                            CASE
                                WHEN url LIKE '%utm_source%' THEN regexp_substr(url, 'utm_source=([^&]+)')
                                WHEN url LIKE '%src%' THEN regexp_substr(url, 'src=([^&]+)')
                            END,
                            'utm_source=|src=',
                            ''
                        ) AS parameter_value
                    FROM
                        {company_id}
                    WHERE
                        url LIKE '%utm_source%'
                        OR url LIKE '%src%';
                            """)
        elif selection == 'medium':
            query = text(f"""
                    SELECT DISTINCT
                        regexp_replace(regexp_substr(url, 'utm_medium=([^&]+)'), 'utm_medium=', '') AS utm_medium
                    FROM
                        {company_id}
                    WHERE
                        url LIKE '%utm_medium%';
                            """)
        elif selection == 'name':
            query = text(f"""
                    SELECT DISTINCT
                        regexp_replace(regexp_substr(url, 'utm_campaign=([^&]+)'), 'utm_campaign=', '') AS utm_campaign
                    FROM
                        {company_id}
                    WHERE
                        url LIKE '%utm_campaign%';
                            """)
            
    with ENGINE.connect() as connection:
        result = connection.execute(query)
        result_list = [row[0] for row in result.fetchall()]
        print(result_list)
        return jsonify(result_list)




@app.route("/api/get-compare-data", methods=['POST'])
def get_compare_data():
    data = request.get_json()
    company_id = data.get('company_id')
    filters = data.get('filters')
    segment_id = data.get('segment_id')
    selection_one = data.get('selection_one')
    selection_two = data.get('selection_two')
    compare_type = data.get('compare_type')
    url_selection = data.get('url_selection')
    utm_selection = data.get('utm_selection')
    card_name_dict = {
        "age" : 'Average Age',
        "net_worth" : "Average Net Worth",
        "credit_range": "Average Credit Range",
        "income_levels": "Average Income Levels",
        "home_price" : "Average Home Price",
        "home_value" : "Average Home Value",
        "mortgage_amount" : "Average Mortgage Amount",
        "mortgage_age": "Average Mortgage Age",
        "ethnic_group" : "Ethnic Group By Percent",
        "journey": "Total Journeys Captured",
        "people": "Total Users Captured"
    }

    average_columns = [
    'age',
    'net_worth',
    'credit_range',
    'income_levels',
    'home_value',
    'mortgage_amount',
    'mortgage_age',
    ]

    by_percent_columns = [
        'ethnic_group',
    ]


    average_data_dict_one = {}
    average_data_dict_two = {}

    average_final_list = []
    by_percent_final_list = []

    utm_filters_dict_one = [{
        "id": "contains",
        "value" : {
            "include_array" : [{"value": f'={value}', "type": 'contain'} for value in utm_selection["selection_one"]["values"]],
            "exclude_array": []
        }
    }]

    utm_filters_dict_two = [{
        "id": "contains",
        "value" : {
            "include_array" : [{"value": f'={value}', "type": 'contain'} for value in utm_selection["selection_two"]["values"]],
            "exclude_array": []
        }
    }]


    filters_dict_one = [{
        "id": "contains",
        "value" : {
            "include_array" : [
                {"value": url_selection["selection_one"]["val"], "type": url_selection["selection_one"]["prefix"]},
            ],
            "exclude_array": []
        }
    }]
    filters_dict_two = [{
        "id": "contains",
        "value" : {
            "include_array" : [
                {"value": url_selection["selection_two"]["val"], "type": url_selection["selection_two"]["prefix"]}
            ],
            "exclude_array": []
        }
    }]

    filter_utm_one = build_filter(company_id,utm_filters_dict_one)
    filter_utm_two = build_filter(company_id,utm_filters_dict_two)

    if filters is not None:
        if filter_utm_one is not None:
            if compare_type == 'Date':
                selection_one_date_query = build_date_query(selection_one["startDate"],selection_one["endDate"])
                selection_two_date_query = build_date_query(selection_two["startDate"],selection_two["endDate"])
                filter_query_one = text(f'{build_filter(company_id,filters)} AND {filter_utm_one}')
                filter_query_two = text(f'{build_filter(company_id,filters)} AND {filter_utm_two}')

            elif compare_type == 'URL':
                filter_query_one = text(f'{build_filter(company_id,filters_dict_one)} AND {build_filter(company_id,filters)} AND {filter_utm_one}')
                filter_query_two = text(f'{build_filter(company_id,filters_dict_two)} AND {build_filter(company_id,filters)} AND {filter_utm_two}')
                selection_one_date_query = build_date_query()
                selection_two_date_query = build_date_query()
        else:
            if compare_type == 'Date':
                selection_one_date_query = build_date_query(selection_one["startDate"],selection_one["endDate"])
                selection_two_date_query = build_date_query(selection_two["startDate"],selection_two["endDate"])
                filter_query_one = build_filter(company_id,filters)
                filter_query_two = build_filter(company_id,filters)

            elif compare_type == 'URL':
                filter_query_one = text(f'{build_filter(company_id,filters_dict_one)} AND {build_filter(company_id,filters)}')
                filter_query_two = text(f'{build_filter(company_id,filters_dict_two)} AND {build_filter(company_id,filters)}')
                selection_one_date_query = build_date_query()
                selection_two_date_query = build_date_query()
    else:
        if utm_selection is not None:
            if compare_type == 'Date':
                selection_one_date_query = build_date_query(selection_one["startDate"],selection_one["endDate"])
                selection_two_date_query = build_date_query(selection_two["startDate"],selection_two["endDate"])
                filter_query_one = filter_utm_one
                filter_query_two = filter_utm_two

            elif compare_type == 'URL':
                filter_query_one = text(f'{build_filter(company_id,filters_dict_one)} AND {filter_utm_one}')
                filter_query_two = text(f'{build_filter(company_id,filters_dict_two)} AND {filter_utm_two}')
                selection_one_date_query = build_date_query()
                selection_two_date_query = build_date_query()
        else:
            if compare_type == 'Date':
                selection_one_date_query = build_date_query(selection_one["startDate"],selection_one["endDate"])
                selection_two_date_query = build_date_query(selection_two["startDate"],selection_two["endDate"])
                filter_query_one = None
                filter_query_two = None

            elif compare_type == 'URL':
                filter_query_one = build_filter(company_id,filters_dict_one)
                filter_query_two = build_filter(company_id,filters_dict_two)
                selection_one_date_query = build_date_query()
                selection_two_date_query = build_date_query()



    for value in by_percent_columns:
        item_list_one, count_list_one = get_by_precent_count(ENGINE, company_id, value, selection_one_date_query,filter_query_one)
        item_list_two, count_list_two = get_by_precent_count(ENGINE, company_id, value, selection_two_date_query,filter_query_two)

        try:
            percent_list = []
            for index, item in enumerate(count_list_one):
                original_value = count_list_two[index]
                new_value = item

                percentage_change = calculate_percentage_change(original_value, new_value)

                percent_list.append(percentage_change)
        except Exception:
            pass
        
        by_percent_final_list.append({
            'item_list': item_list_one,
            'item_count_list': count_list_one,
            'percent_change': percent_list,
            'obj_name' : card_name_dict[value]
        })

    journey_count_one, people_count_one = get_counts(ENGINE,company_id,selection_one_date_query,filter_query_one)
    journey_count_two, people_count_two = get_counts(ENGINE,company_id,selection_two_date_query,filter_query_two)
    original_value_journey = journey_count_two
    original_value_people = people_count_two
    new_value_journey = journey_count_one
    new_value_people = people_count_one


    percentage_change_journey = calculate_percentage_change(original_value_journey, new_value_journey)
    percentage_change_people = calculate_percentage_change(original_value_people, new_value_people)


    average_final_list.append({
        'card_value': format(new_value_journey,","),
        'card_compare_percent': percentage_change_journey,
        'card_name' : card_name_dict['journey']
    })

    average_final_list.append({
        'card_value': format(new_value_people,","),
        'card_compare_percent': percentage_change_people,
        'card_name' : card_name_dict['people']
    })

    for value in average_columns:
        average_data_dict_one[value] = get_average(ENGINE,value,company_id,selection_one_date_query,filter_query_one)
        average_data_dict_two[value] = get_average(ENGINE,value,company_id,selection_two_date_query,filter_query_two)

    for key in average_data_dict_one:
        obj = {}
        if key in average_data_dict_two:
            original_value = average_data_dict_two[key]
            new_value = average_data_dict_one[key]

            percentage_change = calculate_percentage_change(original_value, new_value)

            # Ensure the key exists in final_dict or create it

            # Update final_dict with card_value and card_compare_percent
            obj["card_value"] = str(new_value)
            obj["card_compare_percent"] = percentage_change
            obj["card_name"] = card_name_dict[key]
            average_final_list.append(obj)


    response = {
        'average_list' : average_final_list,
        'by_percent_list': by_percent_final_list
    }




    return jsonify(response)


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
                USER_COLLECTION.update_one({"user_email": user_email}, {
                                           "$addToSet": {"companies": company_id}})
            for user_email in old_company["attached_users"]:
                if user_email not in new_company_data.get('attached_users', []):
                    USER_COLLECTION.update_one({'user_email': user_email}, {
                                               '$pull': {'companies': company_id}})
            return jsonify({"message": "Company updated successfully"})
        else:
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

        SEGMENT_COLLECTION.update_one(
            {"_id": segment_id}, {'$set': new_segment_data})

        return jsonify({"message": "Segment updated successfully"})

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401




@app.route("/api/create-integration", methods=['POST'])
def create_integration():
    token = request.headers.get('Authorization')
    try:
        data = request.get_json()
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        company_id = data.get('company_id')
        integration_name = data.get('integration_name')
        api_key = data.get('api_key')
        site_id = data.get('site_id')
        list_id = data.get('list_id')
        code_list = []

        integration_doc = INTEGRATION_COLLECTION.find_one(
            {'name': integration_name})
        if integration_doc:
            # Build and execute the SQL query to select a random row
            query = text(
                f"SELECT DISTINCT ON (full_name) * FROM {company_id} WHERE CAST(date_added AS DATE) = CURRENT_DATE;")

            with ENGINE.connect() as connection:
                result = connection.execute(query)
                column_names = result.keys()
            data = [dict(zip(column_names, row)) for row in result]

            print(data)

            for row in data:
                if integration_name == 'Klaviyo':
                    status_code = klayviyo_integration(api_key, row)
                elif integration_name == 'Customer.io':
                    status_code = customerIO_integration(site_id, api_key, row)
                elif integration_name == 'Mailchimp':
                    status_code = mailchimp_integration(api_key, list_id, row)
                elif integration_name == 'SendGrid':
                    status_code = sendgrid_integration(api_key, row, [list_id])
                elif integration_name == 'HubSpot':
                    status_code = hubspot_integration(api_key, row)
                elif integration_name == 'EmailOctopus':
                    status_code = emailoctopus_integration(
                        api_key, list_id, row)
                elif integration_name == 'OmniSend':
                    status_code = omnisend_integration(api_key, row)  # done
                code_list.append(status_code)

            print(code_list)
            return jsonify(200)

        else:
            return jsonify(404)
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/test-integration", methods=['POST'])
def test_integration():
    token = request.headers.get('Authorization')
    try:
        data = request.get_json()
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        company_id = data.get('company_id')
        integration_name = data.get('integration_name')
        api_key = data.get('api_key')
        site_id = data.get('site_id')
        list_id = data.get('list_id')
        client_secret = data.get('client_secret')
        public_id = data.get('public_id')
        filters = data.get('filters')

        integration_doc = INTEGRATION_COLLECTION.find_one(
            {'name': integration_name})
        if integration_doc:
            # Build and execute the SQL query to select a random row
            if filters:
                filter_query = build_filter(company_id, filters)
                query = text(
                    f"SELECT * FROM {company_id} WHERE {filter_query} ORDER BY random() LIMIT 1;")
            else:
                query = text(
                    f"SELECT * FROM {company_id} ORDER BY random() LIMIT 1;")

            with ENGINE.connect() as connection:
                result = connection.execute(query)
                random_row = result.fetchone()
                column_names = result.keys()
            row = dict(zip(column_names, random_row))

            if integration_name == 'Klaviyo':
                status_code = klayviyo_integration(api_key, row)  # done
            elif integration_name == 'BayEngage':
                if client_secret and public_id:
                    status_code = bayengage_integration(
                        client_secret, public_id, row, list_id)
                else:
                    status_code = 500
            elif integration_name == 'Customer.io':
                status_code = customerIO_integration(
                    site_id, api_key, row)  # done
            elif integration_name == 'Mailchimp':
                status_code = mailchimp_integration(
                    api_key, list_id, row)  # done
            elif integration_name == 'SendGrid':
                status_code = sendgrid_integration(
                    api_key, row, [list_id])  # done
            elif integration_name == 'HubSpot':
                status_code = hubspot_integration(api_key, row)  # done
            elif integration_name == 'EmailOctopus':
                status_code = emailoctopus_integration(
                    api_key, list_id, row, test=True)  # done
            elif integration_name == 'OmniSend':
                status_code = omnisend_integration(api_key, row)  # done

            print(status_code)

            if status_code in [200, 201, 202]:
                return jsonify(200)
            else:
                return jsonify(500)
        else:
            return jsonify(404)
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        return jsonify({"error": "Invalid token"}), 401



@app.route("/api/get-sync-progress", methods=['POST'])
def get_sync_progress():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
        data = request.get_json()
        segment_id = data.get('segment_id')
        index = data.get('index')

        if segment_id is None:
            integration = USER_COLLECTION.find_one({'_id' : user_id})["integrations"][index]
        else:
            integration = SEGMENT_COLLECTION.find_one({'_id' : segment_id})["integrations"][index]

        sync_progress = integration["sync_progress"]

        if sync_progress == 0 and integration["sync_started"] == 1:
            sync_progress = 1


        response = {
            'sync_started' : integration["sync_started"],
            'sync_progress': sync_progress,
        }


        return jsonify(response)


    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401
    

@app.route("/api/stop-sync-progress", methods=['POST'])
def stop_sync_progress():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
        data = request.get_json()
        segment_id = data.get('segment_id')
        index = data.get('index')

        if segment_id is None:
            USER_COLLECTION.update_one({'_id' : user_id}, {'$set': {f"integrations.{index}.sync_started" : 0}})
        else:
            SEGMENT_COLLECTION.update_one({'_id' : segment_id}, {'$set': {f"integrations.{index}.sync_started" : 0}})


        return jsonify('done')

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401


@app.route("/api/delete-integration", methods=['DELETE'])
def delete_integration():
    integration_id = request.args.get('integration_id')
    segment_id = request.args.get('segment_id')


    if segment_id:
        SEGMENT_COLLECTION.update_one(
            {'integrations.id': integration_id},
            {'$pull': {'integrations': {'id': integration_id}}}
        )
    else:
        USER_COLLECTION.update_one(
            {'integrations.id': integration_id},
            {'$pull': {'integrations': {'id': integration_id}}}
        )
    return jsonify('done!')

    
@app.route("/api/sync-user-integration", methods=['POST'])
def sync_user_integration():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
        data = request.get_json()
        company_id = data.get('company_id')
        segment_id = data.get('segment_id')
        integration_name = data.get('integration_name')
        api_key = data.get('api_key')
        site_id = data.get('site_id')
        list_id = data.get('list_id')
        client_secret = data.get('client_secret')
        public_id = data.get('public_id')
        filters = data.get('filters')
        index = data.get('index')

        filter_query = build_filter(company_id,filters)

        scheduler.add_job(func=sync_integration, misfire_grace_time=15*60, next_run_time=datetime.now(),
                                args=[ENGINE,USER_COLLECTION,company_id,user_id,index,api_key,integration_name,client_secret,public_id,site_id,list_id,filter_query,segment_id,SEGMENT_COLLECTION])

        return jsonify({'status': 'yes'})

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        print(e)
        return jsonify({"error": "Invalid token"}), 401


    

@app.route("/api/get-profile-picture", methods=['GET'])
def get_profile_picture():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']

                # Assuming image file name is based on user_id
        image_path = f"/companies/profile_images/{user_id}.png"

        return send_file(image_path, mimetype='image/png')
    
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        return jsonify({"error": "Invalid token"}), 401
    


@app.route("/api/upload-profile-picture", methods=['POST'])
def upload_profile_picture():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']

        if 'file' not in request.files:
            return jsonify({'error': 'No file part'})

        file = request.files['file']

        if file.filename == '':
            return jsonify({'error': 'No selected file'})

        upload_profile_picture_blob(AZURE_ACCOUNT_URL,AZURE_CONNECTION_BLOB_STRING,AZURE_CONTAINER_NAME,file,user_id)

        return jsonify({'message': 'File uploaded successfully'})
    
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        return jsonify({"error": "Invalid token"}), 401

@app.route("/api/download-users", methods=['POST'])
def download_users():
    data = request.get_json()
    company_id = data.get('id').lower()
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    download_type = data.get('download_type')
    segment_id = data.get('segment_id')
    segment_name = data.get('segment_name')
    company_name = data.get('company_name')

    if segment_id is not None:
        csv_file_path = f'/companies/{company_id}/{segment_id}-{download_type}.csv'
        download_file_name = f'{company_name}-{segment_name}_segment-{download_type}'
    else:
        csv_file_path = f'/companies/{company_id}/main-{download_type}.csv'
        if not os.path.exists(csv_file_path):
            create_df_file_from_db(
                AZURE_ACCOUNT_URL,
                AZURE_CONNECTION_BLOB_STRING,
                USERS_FILE_COLUMNS_ORDER,
                JOURNEY_FILE_COLUMNS_ORDER,
                ENGINE,
                AZURE_CONTAINER_NAME,
                company_id
            )
        download_file_name = f'{company_name}-{download_type}'

    df = pd.read_csv(csv_file_path)
    df = build_df_date_query(df, start_date, end_date)
    # Create a response with the CSV data
    response = make_response(df.to_csv(index=False))
    response.headers["Content-Disposition"] = f"attachment; filename={download_file_name}.csv"
    response.headers["Content-Type"] = "text/csv"

    return response

@app.route("/api/generate-api-key", methods=['GET'])
def generate_user_api_key():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
        
        api_key = generate_api_key()

        USER_COLLECTION.update_one({"_id" : user_id}, {"$set" : {
            "api_key" : api_key
        }})

        return jsonify(api_key)

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        return jsonify({"error": "Invalid token"}), 401
    
@app.route("/api/get-api-key", methods=['GET'])
def get_user_api_key():
    token = request.headers.get('Authorization')
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']

        user = USER_COLLECTION.find_one({"_id" : user_id})
        if user["api_key"]:
            api_key = user["api_key"]
        else:
            api_key = 'No API Key Yet'

        return jsonify(api_key)

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.DecodeError as e:
        return jsonify({"error": "Invalid token"}), 401




########################################################################################
# FROM HERE ALL ENDPOINTS ARE FOR EXTERNAL USERS TO USE OUR API FOR DATA
########################################################################################


@app.route("/iceapi/get-ids", methods=['GET'])
def get_ids():
    api_key = request.headers.get('API_KEY')
    user = USER_COLLECTION.find_one({"api_key" : api_key})
    if user:
       companies = []
       segments = []
       for company_id in user["companies"]:
           company = COMPANIES_COLLECTION.find_one({"_id" : company_id})
           companies.append({"company_id" : company_id, "company_name" : company["company_name"]})
           segment = SEGMENT_COLLECTION.find_one({"attached_company": company_id})
           if segment:
               segments.append({"segment_id" : segment["_id"], "segment_name": segment["segment_name"]})

       response = {
           "companies": companies,
           "segments" : segments
       }
 
       return jsonify(response)
    else:
        return jsonify('API Key Invalid'), 404

@app.route("/iceapi/get-user-data", methods=['POST'])
def get_user_data():
    api_key = request.headers.get('API_KEY')
    user = USER_COLLECTION.find_one({"api_key" : api_key})
    if user:
        data = request.get_json()
        company_id = data.get('company_id').lower() # Required
        data_type = data.get('data_type') # Required
        start_date = data.get('start_date') # Optional
        end_date = data.get('end_date') # Optional
        segment_id = data.get('segment_id') # Optional

        date_range_query = build_date_query(start_date, end_date)

        if segment_id:
            segment = SEGMENT_COLLECTION.find_one({"_id" : segment_id})
            filter_query = build_filter(company_id, segment["filters"])
            if data_type == 'journey':
                query = text(
                        f"SELECT  * FROM {company_id} WHERE ({date_range_query} AND {filter_query});")
            elif data_type == 'users':
                query = text(
                        f"SELECT DISTINCT ON (full_name) * FROM {company_id} WHERE ({date_range_query} AND {filter_query});")
        else:
            if data_type == 'journey':
                query = text(
                        f"SELECT  * FROM {company_id} WHERE {date_range_query};")
            elif data_type == 'users':
                query = text(
                        f"SELECT DISTINCT ON (full_name) * FROM {company_id} WHERE {date_range_query};")

        with ENGINE.connect() as connection:
            data = connection.execute(query)
            colum_list = data.keys()
            result = [dict(zip(colum_list, row)) for row in data]

        # Return data as JSON
        return jsonify(result)
    else:
        return jsonify('API Key Invalid'), 404






@app.errorhandler(500)
def internal_error(error):

    print(error)

    return jsonify("Not Found")






# @app.route("/api/add-data", methods=['POST'])
# def add_data():
#     company_id = request.args.get('id')
#     company_collection = DB_CLIENT[f'{company_id}_data']

#     df = pd.read_csv(
#         '/Users/neevassouline/Desktop/Coding Projects/IcewebIO/backend/people_data_20.csv')
#     df.fillna('-', inplace=True)

#     df[['date', 'hour']] = df['date'].str.split(' ', expand=True)
#     df[['hour']] = df['hour'].str.split('+', expand=True)[[0]]
#     df['full_name'] = df['firstName'] + ' ' + df['lastName']

#     list_of_lists = df.to_dict(orient='records')
#     company_collection.insert_many(list_of_lists)

#     return jsonify('Done!')


# from google.auth.transport.requests import Request
# from google.auth.credentials import AnonymousCredentials
# from google.auth.transport.requests import Request
# from google.auth import jwt
# from google.ads.googleads.client import GoogleAdsClient

# @app.route("/api/auth/google", methods=['POST'])
# def auth_google():
#     code = request.json.get('code')
#     client_id = '852749625849-0711o5c0mn6elckm3cqllf546am5u58d.apps.googleusercontent.com'
#     client_secret = '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'
#     redirect_uri = 'http://localhost:3000'
#     grant_type = 'authorization_code'

#     token_url = 'https://oauth2.googleapis.com/token'
#     headers = {'Content-Type': 'application/x-www-form-urlencoded'}
#     data = {
#         'code': code,
#         'client_id': client_id,
#         'client_secret': client_secret,
#         'redirect_uri': redirect_uri,
#         'grant_type': grant_type,
#     }

#     try:
#         response = requests.post(token_url, headers=headers, data=data)
#         response.raise_for_status()
#         tokens = response.json()


#         credentials = {
#                 "developer_token": "A37QyGuX6bLSUzyCMrfvbA",
#                 "refresh_token": tokens['refresh_token'],
#                 "client_id": client_id,
#                 "client_secret": client_secret,
#                 "use_proto_plus": False}

#         print(credentials)
#         client = GoogleAdsClient.load_from_dict(credentials)
#         # Send the tokens back to the frontend, or store them securely and create a session

#         create_customer_match_user_list(client, '2315440060')
#         return jsonify(tokens)
#     except requests.exceptions.RequestException as e:
#         # Handle errors in the token exchange
#         print('Token exchange error:', e)
#         return jsonify({'error': 'Internal Server Error'})

# def update_excluded_users(segment_id):
#     segment = SEGMENT_COLLECTION.find_one({'_id': segment_id})
#     if segment:
#         filters = segment['filters']
#         company_id = segment['attached_company']
#         collection = DB_CLIENT[f'{company_id}_data']
#         user_ids_to_exclude = set()

#         # Compile regex patterns for different types outside the loop
#         regex_patterns = {
#             'contain': (lambda value: re.compile(re.escape(value), re.IGNORECASE)),
#             'start': (lambda value: re.compile(f"^{re.escape(value)}", re.IGNORECASE)),
#             'end': (lambda value: re.compile(f"{re.escape(value)}$", re.IGNORECASE)),
#         }

#         for filter_item in filters:
#             if filter_item["id"] == 'contains':
#                 exclude_array = filter_item.get("value", {}).get('exclude_array', [])
#                 querys = []
#                 for exclude in exclude_array:
#                     exclude_value = exclude.get('value')
#                     exclude_type = exclude.get('type', 'contain')
#                     exclude_regex = regex_patterns.get(exclude_type, regex_patterns['contain'])(exclude_value)
#                     querys.append({"url": {"$regex": exclude_regex}})
#                 excluded_users = collection.find({"$or": querys})
#                 user_ids_to_exclude.update(user.get('fullName') for user in excluded_users)

#         SEGMENT_COLLECTION.update_one(
#             {'_id': segment_id},
#             {'$set': {'excluded_users': list(user_ids_to_exclude)}}
#         )
#     else:
#         print("Segment not found or an error occurred.")

    # date_range_query = build_date_query(start_date, end_date)

    # else:
    #     base_query = {
    #         '$and': [date_range_query, filter_query]
    #     }

    #     if excluded_users:
    #         base_query['fullName'] = {"$nin": excluded_users}
    #     else:
    #         if is_segnew:
    #             filters = filters[1]
    #         if filters:
    #             try:
    #                 user_ids_to_exclude = set()
    #                 # Compile regex patterns for different types outside the loop
    #                 regex_patterns = {
    #                     'contain': (lambda value: re.compile(re.escape(value), re.IGNORECASE)),
    #                     'start': (lambda value: re.compile(f"^{re.escape(value)}", re.IGNORECASE)),
    #                     'end': (lambda value: re.compile(f"{re.escape(value)}$", re.IGNORECASE)),
    #                 }

    #                 user_ids_to_exclude = set()

    #                 for filter in filters:
    #                     if filter["id"] == 'contains':
    #                         querys = []
    #                         exclude_array = filter["value"].get('exclude_array', [])
    #                         for exclude in exclude_array:
    #                             exclude_value = exclude['value']
    #                             exclude_type = exclude['type'] or 'contain'
    #                             exclude_regex = regex_patterns.get(exclude_type, regex_patterns['contain'])(exclude_value)
    #                             querys.append({"url": {"$regex": exclude_regex}})
    #                         excluded_users = collection.find({"$or": querys})
    #                         user_ids_to_exclude.update(user['fullName'] for user in excluded_users)

    #                 if user_ids_to_exclude:
    #                     base_query['fullName'] = {"$nin": list(user_ids_to_exclude)}
    #             except KeyError:
    #                 pass

    # instance = collection.find(base_query).skip(skip).limit(ITEMS_PER_PAGE)

    # # Apply sorting if provided
    # if sorting:
    #     # Add sorting logic based on the sorting parameter
    #     for option in sorting:
    #         if option["desc"] == "False":
    #             instance = instance.sort([(option["id"], 1)])
    #         elif option["desc"] == "True":
    #             instance = instance.sort([(option["id"], -1)])

    # if user_ids_to_exclude:
    #     response_data = {
    #         'users': json.loads(json_util.dumps(list(instance))),
    #         'excluded_users': list(user_ids_to_exclude)
    #     }
    # else:
    #     response_data = {
    #         'users': json.loads(json_util.dumps(list(instance)))
    #     }

    # return jsonify(response_data)
