from datetime import datetime, timedelta
import apscheduler.jobstores.base
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.combining import OrTrigger
from apscheduler.triggers.cron import CronTrigger
from flask import Flask, request,jsonify,  Response, make_response
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
import re
import threading
import pandas as pd
import psycopg2
from psycopg2 import pool, sql
from sqlalchemy import create_engine, text, MetaData, Table, Column, Text
import os

app = Flask(__name__)
CORS(app)
bcrypt = Bcrypt(app)
scheduler = BackgroundScheduler()
scheduler.start()



# DB_CONNECTOR = MongoClient("mongodb://icewebniv:G3ccZpGQXvs6mVdsYuTCEfk3EuDKTdASUsNyEi6HCFoFp7Af3ESn0asd80pDRIP1w51FILE3QvdYACDbYY0n4g==@icewebniv.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@icewebniv@")

POSTGRES_CONN_STRING = 'host=c-icewebio-postgres.tfgc42d4iqjg72.postgres.cosmos.azure.com port=5432 dbname=citus user=citus password=iceWeb1234 sslmode=require'

database_uri = "postgresql://citus:iceWeb1234@c-icewebio-postgres.tfgc42d4iqjg72.postgres.cosmos.azure.com:5432/citus"

POSTGRES_CONNECTION = psycopg2.pool.SimpleConnectionPool(1, 20, 'host=c-icewebio-postgres.tfgc42d4iqjg72.postgres.cosmos.azure.com port=5432 dbname=citus user=citus password=iceWeb1234 sslmode=require')

ENGINE = create_engine(database_uri, pool_size=20, max_overflow=10)


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

COLUMNS_ORDER = ['date', 'url', 'full_name', 'first_name', 'last_name', 'facebook', 'linked_in', 'twitter', 'email', 'opt_in', 'opt_in_date', 'opt_in_ip', 'opt_in_url', 'pixel_first_hit_date', 'pixel_last_hit_date', 'bebacks', 'phone', 'dnc', 'age', 'gender', 'marital_status', 'address', 'city', 'state', 'zip', 'household_income', 'net_worth', 'income_levels', 'people_in_household', 'adults_in_household', 'children_in_household', 'veterans_in_household', 'education', 'credit_range', 'ethnic_group', 'generation', 'home_owner', 'occupation_detail', 'political_party', 'religion', 'children_between_ages0_3', 'children_between_ages4_6', 'children_between_ages7_9', 'children_between_ages10_12', 'children_between_ages13_18', 'behaviors', 'children_age_ranges', 'interests', 'owns_amex_card', 'owns_bank_card', 'dwelling_type', 'home_heat_type', 'home_price', 'home_purchased_years_ago', 'home_value', 'household_net_worth', 'language', 'mortgage_age', 'mortgage_amount', 'mortgage_loan_type', 'mortgage_refinance_age', 'mortgage_refinance_amount', 'mortgage_refinance_type', 'is_multilingual', 'new_credit_offered_household', 'number_of_vehicles_in_household', 'owns_investment', 'owns_premium_amex_card', 'owns_premium_card', 'owns_stocks_and_bonds', 'personality', 'is_political_contributor', 'is_voter', 'premium_income_household', 'urbanicity', 'maid', 'maid_os', 'hour']


JOURNEY_FILE_COLUMNS_ORDER = ['date', 'hour','full_name','url', 'facebook', 'linked_in', 'twitter', 'email', 'opt_in', 'opt_in_date', 'opt_in_ip', 'opt_in_url', 'pixel_first_hit_date', 'pixel_last_hit_date', 'bebacks', 'phone', 'dnc', 'age', 'gender', 'marital_status', 'address', 'city', 'state', 'zip', 'household_income', 'net_worth', 'income_levels', 'people_in_household', 'adults_in_household', 'children_in_household', 'veterans_in_household', 'education', 'credit_range', 'ethnic_group', 'generation', 'home_owner', 'occupation_detail', 'political_party', 'religion', 'children_between_ages0_3', 'children_between_ages4_6', 'children_between_ages7_9', 'children_between_ages10_12', 'children_between_ages13_18', 'behaviors', 'children_age_ranges', 'interests', 'owns_amex_card', 'owns_bank_card', 'dwelling_type', 'home_heat_type', 'home_price', 'home_purchased_years_ago', 'home_value', 'household_net_worth', 'language', 'mortgage_age', 'mortgage_amount', 'mortgage_loan_type', 'mortgage_refinance_age', 'mortgage_refinance_amount', 'mortgage_refinance_type', 'is_multilingual', 'new_credit_offered_household', 'number_of_vehicles_in_household', 'owns_investment', 'owns_premium_amex_card', 'owns_premium_card', 'owns_stocks_and_bonds', 'personality', 'is_political_contributor', 'is_voter', 'premium_income_household', 'urbanicity', 'maid', 'maid_os']

USERS_FILE_COLUMNS_ORDER = ['date','full_name', 'email','facebook', 'linked_in', 'twitter', 'opt_in', 'opt_in_date', 'opt_in_ip', 'opt_in_url', 'pixel_first_hit_date', 'pixel_last_hit_date', 'bebacks', 'phone', 'dnc', 'age', 'gender', 'marital_status', 'address', 'city', 'state', 'zip', 'household_income', 'net_worth', 'income_levels', 'people_in_household', 'adults_in_household', 'children_in_household', 'veterans_in_household', 'education', 'credit_range', 'ethnic_group', 'generation', 'home_owner', 'occupation_detail', 'political_party', 'religion', 'children_between_ages0_3', 'children_between_ages4_6', 'children_between_ages7_9', 'children_between_ages10_12', 'children_between_ages13_18', 'behaviors', 'children_age_ranges', 'interests', 'owns_amex_card', 'owns_bank_card', 'dwelling_type', 'home_heat_type', 'home_price', 'home_purchased_years_ago', 'home_value', 'household_net_worth', 'language', 'mortgage_age', 'mortgage_amount', 'mortgage_loan_type', 'mortgage_refinance_age', 'mortgage_refinance_amount', 'mortgage_refinance_type', 'is_multilingual', 'new_credit_offered_household', 'number_of_vehicles_in_household', 'owns_investment', 'owns_premium_amex_card', 'owns_premium_card', 'owns_stocks_and_bonds', 'personality', 'is_political_contributor', 'is_voter', 'premium_income_household', 'urbanicity']

ITEMS_PER_PAGE = 13



def query_database(table_name, skip, limit):

        query = text(f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {skip};")

        with ENGINE.connect() as connection:
            result = connection.execute(query)
            data = [dict(zip(COLUMNS_ORDER, row)) for row in result]

        return data



def create_postgres_table(table_name):
    metadata = MetaData()

    table = Table(
        table_name,
        metadata,
        *[Column(column, Text) for column in COLUMNS_ORDER]
    )

    metadata.create_all(ENGINE)
        
    return 'done'

def delete_postgres_table(table_name):
    metadata = MetaData()

    table = Table(table_name, metadata, autoload_with=ENGINE)

    table.drop(ENGINE)

    
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

def get_most_popular(table_name, date_range_query, popular_type):
    item_list = []
    count_list = []

    with ENGINE.connect() as connection:
        if popular_type == 'hour':
            query = text(f"""
                SELECT substr(hour, 1, 2) as hour_part, COUNT(*) as count
                FROM {table_name}
                WHERE {date_range_query}
                GROUP BY hour_part
                ORDER BY count DESC
                LIMIT 10
            """)
            result = connection.execute(query)

            for row in result:
                print(row[0])
                item_list.append(convert_to_user_friendly_time(row[0]))
                count_list.append(row[1])

        else:
            query = text(f"""
                SELECT {popular_type} as item, COUNT(*) as count
                FROM {table_name}
                WHERE {date_range_query}
                GROUP BY {popular_type}
                ORDER BY count DESC
                LIMIT 10
            """)

            result = connection.execute(query)

            for row in result:
                print(row[0])
                item_list.append(row[0])
                count_list.append(row[1])

    return item_list, count_list

def get_by_precent_count(table_name, field, date_range_query):
    item_list = []
    count_list = []

    with ENGINE.connect() as connection:
        query = text(f"""
            SELECT {field} as item, COUNT(*) as count
            FROM {table_name}
            WHERE {date_range_query}
            GROUP BY {field}
            ORDER BY count DESC
            LIMIT 10
        """)

        result = connection.execute(query)

        for row in result:
            if row[0] == '-' or row[0] == 'Unknown':
                pass
            else:
                item_list.append(row[0])
                count_list.append(row[1])

    return item_list, count_list


def get_counts(table_name, date_range_query, filter_query=None):
    with ENGINE.connect() as connection:
        if filter_query is not None:
            query = text(f"""
                SELECT COUNT(DISTINCT "full_name") as total_distinct_count
                FROM {table_name}
                WHERE ({date_range_query} AND {filter_query})
            """)
            result = connection.execute(query).fetchall()
            journey_count = connection.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE {date_range_query}")).scalar()
            try:
                people_count = result[0][0]  # Assuming "full_name" is the first column in the SELECT statement
            except IndexError:
                people_count = 0
        else:
            query_people = text(f"""
                SELECT COUNT(DISTINCT "full_name") as total_distinct_count
                FROM {table_name}
                WHERE {date_range_query}
            """)
            query_journey = text(f"""
                SELECT COUNT(*) as count
                FROM {table_name}
                WHERE {date_range_query}
            """)

            result_people = connection.execute(query_people).fetchall()
            result_journey = connection.execute(query_journey).fetchall()
            try:
                people_count = result_people[0][0]  # Assuming "fullName" is the first column in the SELECT statement
                journey_count = result_journey[0][0]  # Assuming "count" is the first column in the SELECT statement
            except IndexError:
                people_count = 0
                journey_count = 0

    return journey_count, people_count

def get_df_date_query(df, start_date, end_date, search=None):
    print(start_date, end_date)
    # Assume df is your Pandas DataFrame containing the 'date' column
    if start_date in ['undefined', None]:
        start_datetime_to_obj = datetime.today()
        start_datetime = start_datetime_to_obj.strftime('%Y-%m-%d')
        end_datetime_to_obj = datetime.strptime(f'{datetime.now().year}-01-01', '%Y-%m-%d')
        end_datetime = end_datetime_to_obj.strftime('%Y-%m-%d')

        # Create a boolean mask for date range query
        date_range_mask = (df['date'] >= end_datetime) & (df['date'] <= start_datetime)
    else:
        # Parse the start_date and end_date as datetime objects
        start_datetime_to_obj = datetime.strptime(start_date, '%Y-%m-%d')
        start_datetime = start_datetime_to_obj.strftime('%Y-%m-%d')
        end_datetime_to_obj = datetime.strptime(end_date, '%Y-%m-%d')
        end_datetime = end_datetime_to_obj.strftime('%Y-%m-%d')

        # Create a boolean mask for date range query
        date_range_mask = (df['date'] >= start_datetime) & (df['date'] <= end_datetime)

    # Apply the mask to filter rows in the DataFrame
    filtered_df = df.loc[date_range_mask]

    return filtered_df



def get_date_query(start_date, end_date, search=None):
    if any([start_date == 'undefined', start_date is None]):
        start_datetime = datetime(datetime.now().year, 1, 1)
        end_datetime = datetime.today()
    else:
        # Parse the start_date and end_date as datetime objects
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    # Format the datetime objects as strings
    start_date_str = start_datetime.strftime('%Y-%m-%d')
    end_date_str = end_datetime.strftime('%Y-%m-%d')

    print(start_date_str)


    # Create a query to find documents within the specified date range
    date_range_query = text(f"""
        CAST(date AS DATE) >= '{start_date_str}' AND CAST(date AS DATE) <= '{end_date_str}'
    """)

    return date_range_query

def build_df_filter(df,filter_params=None, column_filters=None):
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
                    (column_name, '>=', min_val) & (column_name, '<=', max_val))
            elif filter_key == 'contains':
                or_arr = []
                for include in filter_value['include_array']:
                    try:
                        column_name = 'url'
                        include_value = include['value']
                        include_type = include['type']
                        if include_type == 'contain':
                            or_arr.append(df[column_name].str.contains(include_value, case=False, regex=True))
                        elif include_type == 'start':
                            or_arr.append(df[column_name].str.startswith(include_value))
                        elif include_type == 'end':
                            or_arr.append(df[column_name].str.endswith(include_value))
                        elif include_type == 'exact':
                            or_arr.append(df[column_name] == include_value)
                        elif include_type == 'notequal':
                            or_arr.append(~df[column_name].str.contains(fr"\{include_value}$", case=False, regex=True))
                    except KeyError:
                        pass

                if len(or_arr) > 0:
                    filters.append(pd.concat(or_arr, axis=1).any(axis=1))

            elif filter_key == 'checkbox':
                column_name = filter_value['name']
                value_arr = filter_value['value']
                or_arr = []

                for value in value_arr:
                    or_arr.append(df[column_name].str.contains(re.escape(value), case=False, regex=True))

                if len(or_arr) > 0:
                    filters.append(pd.concat(or_arr, axis=1).any(axis=1))
            else:
                filters.append(df[filter_key].str.contains(filter_value, case=False, regex=True))

    if column_filters:
        for filter in column_filters:
            filter_key = filter["id"]
            filter_value = filter["value"]
            filters.append(df[filter_key].str.contains(filter_value, case=False, regex=True))

    # Combine filters using '&' for multiple filters
    if filters:
        return pd.concat(filters, axis=1).all(axis=1)
    else:
        return pd.Series(True, index=df.index)  # No filters
    

def build_filter(company_id,filter_params=None, column_filters=None):
    filters = []
    exclude_array = []

    if filter_params:
        for filter in filter_params:
            filter_key = filter["id"]
            filter_value = filter["value"]

            if filter_key == 'range':
                column_name = filter_value['name']
                min_val = int(filter_value['minVal'])
                max_val = int(filter_value['maxVal'])

                filters.append(
                    text(f"{column_name} >= '{min_val}' AND {column_name} <= '{max_val}'")
                )
            elif filter_key == 'contains':
                or_arr = []
                for include in filter_value['include_array']:
                    try:
                        column_name = 'url'
                        include_value = include['value']
                        include_type = include['type']

                        if include_type == 'contain':
                            or_arr.append(
                                text(f"{column_name} ILIKE '%{include_value}%'")
                            )
                        elif include_type == 'start':
                            or_arr.append(
                                text(f"{column_name} ILIKE '{include_value}%'")
                            )
                        elif include_type == 'end':
                            or_arr.append(
                                text(f"{column_name} ILIKE '%{include_value}'")
                            )
                        elif include_type == 'exact':
                            or_arr.append(
                                text(f"{column_name} = '{include_value}'")
                            )
                        elif include_type == 'notequal':
                            or_arr.append(
                                text(f"{column_name} NOT ILIKE :include_value").bindparams(include_value=f'%{include_value}%')
                            )
                    except KeyError:
                        pass

                if or_arr:
                    filters.append(f"({text(' OR '.join([str(expr) for expr in or_arr]))})")
                
                or_arr = []
                for exclude in filter_value['exclude_array']:
                    try:
                        column_name = 'url'
                        exclude_value = exclude['value']
                        exclude_type = exclude['type']

                        if exclude_type == 'contain':
                            exclude_array.append(
                                text(f"{column_name} ILIKE '%{exclude_value}%'")
                            )
                        elif exclude_type == 'start':
                            exclude_array.append(
                                text(f"{column_name} ILIKE '{exclude_value}%'")
                            )
                        elif exclude_type == 'end':
                            exclude_array.append(
                                text(f"{column_name} ILIKE '%{exclude_value}'")
                            )
                        elif exclude_type == 'exact':
                            exclude_array.append(
                                text(f"{column_name} = '{exclude_value}'")
                            )
                    except KeyError:
                        pass

            elif filter_key == 'checkbox':
                column_name = filter_value['name']
                value_arr = filter_value['value']
                or_arr = []

                print('chekcvox')
                for value in value_arr:
                    or_arr.append(text(f"{column_name} = '{value}'"))

                if or_arr:
                    filters.append(text(' OR '.join([str(expr) for expr in or_arr])))
            else:
                filters.append(
                    text(f"{filter_key} ILIKE '%{filter_value}%")
                )

    if column_filters:
        for filter in column_filters:
            filter_key = filter["id"]
            filter_value = filter["value"]
            filters.append(
                text(f"{filter_key} ILIKE '%{filter_value}%'")
            )

    # Combine filters using 'AND' for multiple filters
    if filters:
        if len(exclude_array) > 0:
            return text(f"""
            {' AND '.join([str(expr) for expr in filters])}
            AND
            full_name NOT IN (SELECT full_name FROM public.{company_id} WHERE {' OR '.join([str(expr) for expr in exclude_array])}
            )
            """)
        else:
            return text(' AND '.join([str(expr) for expr in filters]))
    else:
        return None  # No filters
    



def update_company_counts(company_id):
        try:
            with ENGINE.connect() as connection:
                journey_count = connection.execute(text(f"SELECT COUNT(*) FROM {company_id}")).scalar()
                people_count = connection.execute(text(f"SELECT COUNT(DISTINCT full_name) as count FROM {company_id}")).scalar()

        except IndexError:
            people_count = 0
            journey_count = 0
        
        print(journey_count)
        print(people_count)

        COMPANIES_COLLECTION.update_one({'_id': company_id}, {'$set' : {'counts' : {
            "people_count" : people_count,
            "journey_count" : journey_count
        }}})
        

def update_popular_chart(company_id):
    popular_chart = {}
    popular_types = ['hour', 'url']

    with ENGINE.connect() as connection:
        for popular_type in popular_types:
            item_list = []
            count_list = []

            if popular_type == 'hour':
                query = text(f"""
                    SELECT SUBSTRING(hour, 1, 2) AS hour,
                           COUNT(*) AS count
                    FROM {company_id}
                    GROUP BY hour
                    ORDER BY count DESC
                    LIMIT 10
                """)
            elif popular_type == 'url':
                query = text(f"""
                    SELECT url,
                           COUNT(*) AS count
                    FROM {company_id}
                    GROUP BY url
                    ORDER BY count DESC
                    LIMIT 10
                """)

            result = connection.execute(query).fetchall()

            for item in result:
                item_list.append(item[0])
                count_list.append(item[1])

            popular_chart[popular_type] = {
                'item_list': item_list,
                'count_list': count_list
            }

    COMPANIES_COLLECTION.update_one({'_id': company_id}, {'$set': {'popular_chart': popular_chart}})

def update_by_percent(company_id):
    by_percent_chart = {}
    fields = ['age', 'gender']

    with ENGINE.connect() as connection:
        for field in fields:
            item_list = []
            count_list = []

            query = text(f"""
                SELECT {field},
                       COUNT(*) AS count
                FROM {company_id}
                GROUP BY {field}
                ORDER BY count DESC
                LIMIT 10
            """)

            result = connection.execute(query).fetchall()

            for item in result:
                if item[0] == '-' or item[0] == 'Unknown':
                    pass
                else:
                    item_list.append(item[0])
                    count_list.append(item[1])

            by_percent_chart[field] = {
                'item_list': item_list,
                'count_list': count_list
            }

    COMPANIES_COLLECTION.update_one({'_id': company_id}, {'$set': {'by_percent_chart': by_percent_chart}})

def create_df_file_from_db(company_id,segment_id = None,filters=None):

    directory_path = f'/companies/{company_id}'
    file_types = ['users', 'journey']

    if segment_id == None:
        for file_type in file_types:
            file_name = f'main-{file_type}.csv'
            if file_type == 'users':
                column_names_str = ', '.join(USERS_FILE_COLUMNS_ORDER)
                query = text(f"SELECT DISTINCT ON (full_name) {column_names_str} FROM {company_id};")

                os.makedirs(directory_path, exist_ok=True)

                file_path = os.path.join(directory_path, file_name)

                with ENGINE.connect() as connection:
                    data = connection.execute(query)
                
                df = pd.DataFrame(data, columns=USERS_FILE_COLUMNS_ORDER)
                
            if file_type == 'journey':
                column_names_str = ', '.join(JOURNEY_FILE_COLUMNS_ORDER)

                query = text(f"SELECT {column_names_str} FROM {company_id};")

                os.makedirs(directory_path, exist_ok=True)

                file_path = os.path.join(directory_path, file_name)

                with ENGINE.connect() as connection:
                    data = connection.execute(query)
                
                df = pd.DataFrame(data, columns=JOURNEY_FILE_COLUMNS_ORDER)
                
            csv_content = df.to_csv(index=False)

            with open(file_path, 'w') as file:
                file.write(csv_content)
    else:
        filter_query = build_filter(company_id,filters)
        for file_type in file_types:
            file_name = f'{segment_id}-{file_type}.csv'
            if file_type == 'users':
                
                column_names_str = ', '.join(USERS_FILE_COLUMNS_ORDER)
                query = text(f"""
                    SELECT DISTINCT ON (full_name) {column_names_str}
                    FROM public.{company_id}
                    WHERE ({filter_query})
                """)

                os.makedirs(directory_path, exist_ok=True)

                file_path = os.path.join(directory_path, file_name)

                with ENGINE.connect() as connection:
                    data = connection.execute(query)
                
                df = pd.DataFrame(data, columns=USERS_FILE_COLUMNS_ORDER)
                
            if file_type == 'journey':
                column_names_str = ', '.join(JOURNEY_FILE_COLUMNS_ORDER)

                query = text(f"""
                    SELECT {column_names_str}
                    FROM public.{company_id}
                    WHERE ({filter_query})
                """)

                os.makedirs(directory_path, exist_ok=True)

                file_path = os.path.join(directory_path, file_name)

                with ENGINE.connect() as connection:
                    data = connection.execute(query)
                
                df = pd.DataFrame(data, columns=JOURNEY_FILE_COLUMNS_ORDER)
                
            csv_content = df.to_csv(index=False)

            with open(file_path, 'w') as file:
                file.write(csv_content)




for segment in SEGMENT_COLLECTION.find():
    scheduler.add_job( 
                      func=create_df_file_from_db,
                      trigger=OrTrigger([CronTrigger(hour=19, minute=10)]), 
                      misfire_grace_time=15*60,
                      args=[segment['attached_company'],segment['_id'],segment['filters']]
                    )

for company in COMPANIES_COLLECTION.find():
    scheduler.add_job(
        func=create_df_file_from_db,
        trigger=OrTrigger([CronTrigger(hour=18, minute=0)]), 
        misfire_grace_time=15*60,
        args=[company['_id']]
    )
    scheduler.add_job(
        func=update_company_counts,
        trigger=OrTrigger([CronTrigger(hour=18, minute=30)]), 
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
        func=update_by_percent,
        trigger=OrTrigger([CronTrigger(hour=19, minute=30)]), 
        misfire_grace_time=15*60,
        args=[company['_id']]
    )





@app.route("/api/test", methods=['GET'])
def tes():
    create_df_file_from_db('xs581')

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
        # You can generate a token here and exclude it in the response for future authenticated requests
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
        'attached_users': attached_users,
    }

    USER_COLLECTION.update_many({"user_role": "admin"}, {
                                "$push": {"companies": company_id}})
    COMPANIES_COLLECTION.insert_one(new_company)

    create_postgres_table(company_id.lower())


    update_company_counts(company_id)
    update_popular_chart(company_id)

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
    df['full_name'] = df['firstName'] + ' ' + df['lastName']

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

        segment_id = uuid.uuid4().hex

        new_segment = {
            "_id": segment_id,
            "segment_name": segment_name,
            "attached_company": company_id,
            "created_by": user["user_name"],
            "filters": filters,
        }

        SEGMENT_COLLECTION.insert_one(new_segment)

        create_df_file_from_db(company_id,segment_id,filters)

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
    
    delete_postgres_table(company_id)


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
            filter_query = build_filter(company_id,segment["filters"])
            print(filter_query)
            people_count = get_counts(company_id,get_date_query(None,None),filter_query=filter_query)[1]
            print(people_count)
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


@app.route("/api/get-data", methods=['POST'])
def get_data():
    data = request.get_json()
    company_id = data.get('id').lower()
    start_date = data.get('start-date')
    end_date = data.get('end-date')
    page = data.get('page')
    column_filters = data.get('column_filter')
    filters = data.get('filters')


    try:
        if len(filters) > 0:
            filter_query = build_filter(company_id,filters, column_filters)

        elif len(filters) == 0:
            return jsonify('ignore')
    except TypeError:
        try:
            if filters[0] == 'segnew':
                filter_query = build_filter(company_id,filter_params=filters[1])
        except IndexError:
            filter_query = build_filter(
                company_id,filter_params=None, column_filters=column_filters)
        except TypeError:
            filter_query = build_filter(
                company_id,filter_params=None, column_filters=column_filters)


    skip = page * ITEMS_PER_PAGE

    date_range_query = get_date_query(start_date,end_date)

    if filter_query is None:
        query = text(f"SELECT * FROM {company_id} WHERE ({date_range_query}) LIMIT {ITEMS_PER_PAGE} OFFSET {skip};")
    else:

        # Final query
        query = text(f"""
            SELECT *
            FROM public.{company_id}
            WHERE ({date_range_query} AND {filter_query})
            LIMIT {ITEMS_PER_PAGE} OFFSET {skip}
        """)

        print(str(query))

    # query = text(f"SELECT * FROM {company_id} LIMIT {ITEMS_PER_PAGE} OFFSET {skip};")

    with ENGINE.connect() as connection:
        data = connection.execute(query)
        result = [dict(zip(COLUMNS_ORDER, row)) for row in data]

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
        try:

            if any([start_date == 'undefined', start_date == None]):
                company = COMPANIES_COLLECTION.find_one({'_id': company_id})
                counts = company['counts']
                response = {
                    'people_count': counts['people_count'],
                    'journey_count': counts['journey_count'],
                }
            else:
                date_range_query = get_date_query(start_date, end_date)

                journey_count, people_count = get_counts(company_id,date_range_query)

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
        
        try:
            if any([start_date == 'undefined', start_date == None]):
                company = COMPANIES_COLLECTION.find_one({'_id': company_id})
                by_precent_chart = company['by_precent_chart'][field]
                response = {
                    'items': by_precent_chart['item_list'],
                    'counts': by_precent_chart['count_list']
                }
            else:
                date_range_query = get_date_query(start_date, end_date)


                item_list, count_list = get_by_precent_count(company_id,field,date_range_query)

                response = {
                    'items': item_list,
                    'counts': count_list
                }
        except KeyError:
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

        try:
            if any([start_date == 'undefined', start_date == None]):  
                company = COMPANIES_COLLECTION.find_one({'_id': company_id})
                popular_chart = company["popular_chart"][popular_type]

                response = {
                    'popular_items': popular_chart['item_list'],
                    'popular_items_counts': popular_chart['count_list']
                }
            else:
                date_range_query = get_date_query(start_date, end_date)

                popular_items, popular_items_counts = get_most_popular(company_id,date_range_query,popular_type)

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
            create_df_file_from_db(company_id)
        download_file_name = f'{company_name}-{download_type}'


    df = pd.read_csv(csv_file_path)
    df = get_df_date_query(df,start_date,end_date)
    print(df)
    # Create a response with the CSV data
    response = make_response(df.to_csv(index=False))
    response.headers["Content-Disposition"] = f"attachment; filename={download_file_name}.csv"
    response.headers["Content-Type"] = "text/csv"
    
    return response


@app.errorhandler(500)
def internal_error(error):

    return jsonify("Not Found")


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


    # date_range_query = get_date_query(start_date, end_date)

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
