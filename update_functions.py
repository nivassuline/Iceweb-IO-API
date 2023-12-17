from sqlalchemy import text
from app_functions import *
import re
from integrations import *


def update_company_counts(ENGINE,company_id,COMPANIES_COLLECTION=None,SEGMENT_COLLECTION=None,segment_id=None,filter_query=None):
        if segment_id:
            try:
                with ENGINE.connect() as connection:
                    journey_count = connection.execute(text(f"SELECT COUNT(*) FROM {company_id} WHERE {filter_query}")).scalar()
                    people_count = connection.execute(text(f"SELECT COUNT(DISTINCT full_name) as count FROM {company_id} WHERE {filter_query}")).scalar()

            except IndexError:
                people_count = 0
                journey_count = 0

            SEGMENT_COLLECTION.update_one({'_id': segment_id}, {'$set' : {'counts' : {
                "people_count" : people_count,
                "journey_count" : journey_count
            }}})
        else:
            try:
                with ENGINE.connect() as connection:
                    journey_count = connection.execute(text(f"SELECT COUNT(*) FROM {company_id}")).scalar()
                    people_count = connection.execute(text(f"SELECT COUNT(DISTINCT full_name) as count FROM {company_id}")).scalar()

            except IndexError:
                people_count = 0
                journey_count = 0

            COMPANIES_COLLECTION.update_one({'_id': company_id}, {'$set' : {'counts' : {
                "people_count" : people_count,
                "journey_count" : journey_count
            }}})


def update_popular_chart(ENGINE,company_id,SEGMENT_COLLECTION=None, COMPANIES_COLLECTION=None,segment_id=None, filter_query=None):
    popular_chart = {}
    popular_types = ['hour', 'url','state','income_levels']

    if segment_id:
        with ENGINE.connect() as connection:
            for popular_type in popular_types:
                item_list = []
                count_list = []

                if popular_type == 'hour':
                    query = text(f"""
                    SELECT substr(hour, 1, 2) as hour_part, COUNT(*) as count
                    FROM {company_id}
                    WHERE {filter_query}
                    GROUP BY hour_part
                    ORDER BY count DESC
                    LIMIT 10
                    """)

                    result = connection.execute(query).fetchall()

                    for item in result:
                        item_list.append(convert_to_user_friendly_time(item[0]))
                        count_list.append(item[1])

                elif popular_type == 'state':
                    query = text(f"""
                        SELECT {popular_type} as item, COUNT(*) as count
                        FROM {company_id}
                        WHERE {filter_query}
                        GROUP BY {popular_type}
                        ORDER BY count DESC
                        """)

                    result = connection.execute(query)

                    for row in result:
                        item_list.append(row[0])
                        count_list.append(row[1])

                elif popular_type == 'url':
                    query = text(f"""
                        SELECT url,
                                COUNT(*) AS count
                        FROM {company_id}
                        WHERE {filter_query}
                        GROUP BY url
                        ORDER BY count DESC
                        LIMIT 10
                    """)

                    result = connection.execute(query).fetchall()

                    for item in result:
                        item_list.append(item[0])
                        count_list.append(item[1])


                elif popular_type == 'income_levels':
                    total = 0
                    count_list_new = []
                    query = text(f"""
                        SELECT {popular_type} as item, COUNT(*) as count
                        FROM {company_id}
                        WHERE {filter_query}
                        GROUP BY {popular_type}
                        ORDER BY count DESC
                        LIMIT 10
                        """)

                    result = connection.execute(query).fetchall()
                    for row in result:
                        if row[0] == '-' or 'Unknown' in row[0]:
                            pass
                        else:
                            numbers = re.findall(r'\d+', row[0])
                            print(numbers)
                            if '150' in numbers and len(numbers) == 1:
                                item_list.append('Greater Then 150K')
                            elif '30' in numbers and len(numbers) == 1:
                                item_list.append('Less Then 30K')
                            elif len(numbers) == 2:
                                item_list.append(f'{numbers[0]}K-{numbers[1]}K')
                            total += int(row[1])
                            count_list_new.append(row[1])
                    for count in count_list_new:
                        count_list.append(calculate_percentage(count,total))
                else:
                    for row in result:
                        item_list.append(row[0])
                        count_list.append(row[1])

                popular_chart[popular_type] = {
                    'item_list': item_list,
                    'count_list': count_list
                }

        SEGMENT_COLLECTION.update_one({'_id': segment_id}, {'$set': {'popular_chart': popular_chart}})
    else:
        with ENGINE.connect() as connection:
            for popular_type in popular_types:
                item_list = []
                count_list = []

                if popular_type == 'hour':
                    query = text(f"""
                    SELECT substr(hour, 1, 2) as hour_part, COUNT(*) as count
                    FROM {company_id}
                    GROUP BY hour_part
                    ORDER BY count DESC
                    LIMIT 10
                """)

                    result = connection.execute(query).fetchall()

                    for item in result:
                        print(item)
                        item_list.append(convert_to_user_friendly_time(item[0]))
                        count_list.append(item[1])
                
                elif popular_type == 'state':
                    query = text(f"""
                        SELECT {popular_type} as item, COUNT(*) as count
                        FROM {company_id}
                        GROUP BY {popular_type}
                        ORDER BY count DESC
                        """)

                    result = connection.execute(query)

                    for row in result:
                        item_list.append(row[0])
                        count_list.append(row[1])

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

                elif popular_type == 'income_levels':
                    total = 0
                    count_list_new = []
                    query = text(f"""
                        SELECT {popular_type} as item, COUNT(*) as count
                        FROM {company_id}
                        GROUP BY {popular_type}
                        ORDER BY count DESC
                        LIMIT 10
                        """)

                    result = connection.execute(query).fetchall()
                    for row in result:
                        if row[0] == '-' or 'Unknown' in row[0]:
                            pass
                        else:
                            numbers = re.findall(r'\d+', row[0])
                            print(numbers)
                            if '150' in numbers and len(numbers) == 1:
                                item_list.append('Greater Then 150K')
                            elif '30' in numbers and len(numbers) == 1:
                                item_list.append('Less Then 30K')
                            elif len(numbers) == 2:
                                item_list.append(f'{numbers[0]}K-{numbers[1]}K')
                            total += int(row[1])
                            count_list_new.append(row[1])
                    for count in count_list_new:
                        count_list.append(calculate_percentage(count,total))
                else:
                    for row in result:
                        item_list.append(row[0])
                        count_list.append(row[1])

                popular_chart[popular_type] = {
                    'item_list': item_list,
                    'count_list': count_list
                }
        COMPANIES_COLLECTION.update_one({'_id': company_id}, {'$set': {'popular_chart': popular_chart}})


def update_by_percent(ENGINE,company_id, COMPANIES_COLLECTION=None,SEGMENT_COLLECTION=None,segment_id=None, filter_query=None):
    by_percent_chart = {}
    fields = ['age', 'gender']

    if segment_id:
        with ENGINE.connect() as connection:
            for field in fields:
                item_list = []
                count_list = []
                
                if field == 'age':
                    query = text(f"""
                        SELECT
                        CASE
                            WHEN age = '-' THEN 'Unknown'
                            WHEN age::float < 10 THEN '0-10'
                            WHEN age::float < 20 THEN '10-20'
                            WHEN age::float < 30 THEN '20-30'
                            WHEN age::float < 40 THEN '30-40'
                            WHEN age::float < 50 THEN '40-50'
                            WHEN age::float < 60 THEN '50-60'
                            WHEN age::float < 70 THEN '60-70'
                            WHEN age::float < 80 THEN '70-80'
                            WHEN age::float < 90 THEN '80-90'
                            ELSE 'Unknown'
                        END AS age_range,
                        COUNT(DISTINCT full_name) AS count 
                    FROM
                        {company_id}
                    WHERE 
                        {filter_query}
                    GROUP BY
                        age_range
                    ORDER BY
                        count DESC
                    LIMIT 10;
                        """)
                else:
                    query = text(f"""
                        SELECT {field},
                                COUNT(DISTINCT full_name) AS count 
                        FROM {company_id}
                        WHERE {filter_query}
                        GROUP BY {field}
                        ORDER BY count DESC
                        LIMIT 10
                    """)

                result = connection.execute(query)

                for row in result:
                    if row[0] == '-' or 'Unknown' in row[0]:
                        pass
                    else:
                        if field == 'credit_range':
                            numbers = re.findall(r'\d+', row[0])
                            output_string = '-'.join(numbers)
                            item_list.append(output_string)
                            count_list.append(row[1])
                        else:
                            item_list.append(row[0])
                            count_list.append(row[1])

                    by_percent_chart[field] = {
                        'item_list': item_list,
                        'count_list': count_list
                    }

        SEGMENT_COLLECTION.update_one({'_id': segment_id}, {'$set': {'by_percent_chart': by_percent_chart}})
    else:
        with ENGINE.connect() as connection:
            for field in fields:
                item_list = []
                count_list = []
                
                if field == 'age':
                    query = text(f"""
                        SELECT
                        CASE
                            WHEN age = '-' THEN 'Unknown'
                            WHEN age::float < 10 THEN '0-10'
                            WHEN age::float < 20 THEN '10-20'
                            WHEN age::float < 30 THEN '20-30'
                            WHEN age::float < 40 THEN '30-40'
                            WHEN age::float < 50 THEN '40-50'
                            WHEN age::float < 60 THEN '50-60'
                            WHEN age::float < 70 THEN '60-70'
                            WHEN age::float < 80 THEN '70-80'
                            WHEN age::float < 90 THEN '80-90'
                            ELSE 'Unknown'
                        END AS age_range,
                        COUNT(DISTINCT full_name) AS count 
                    FROM
                        {company_id}
                    GROUP BY
                        age_range
                    ORDER BY
                        count DESC
                    LIMIT 10;
                        """)
                else:
                    query = text(f"""
                        SELECT {field},
                                COUNT(DISTINCT full_name) AS count 
                        FROM {company_id}
                        GROUP BY {field}
                        ORDER BY count DESC
                        LIMIT 10
                    """)

                result = connection.execute(query)

                for row in result:
                    print(row)
                    if row[0] == '-' or 'Unknown' in row[0]:
                        pass
                    else:
                        if field == 'credit_range':
                            numbers = re.findall(r'\d+', row[0])
                            output_string = '-'.join(numbers)
                            item_list.append(output_string)
                            count_list.append(row[1])
                        else:
                            item_list.append(row[0])
                            count_list.append(row[1])

                    by_percent_chart[field] = {
                        'item_list': item_list,
                        'count_list': count_list
                    }

        COMPANIES_COLLECTION.update_one({'_id': company_id}, {'$set': {'by_percent_chart': by_percent_chart}})


def update_integration(ENGINE,company_id,api_key,integration_name,client_secret=None,public_id=None,site_id=None,list_id=None,filter_query=None):
    # Build and execute the SQL query to select a random row
    if filter_query:
        query = text(f"SELECT DISTINCT ON (full_name) * FROM {company_id} WHERE (CAST(date_added AS DATE) = CURRENT_DATE AND {filter_query}) LIMIT 10;")
    else:
        query = text(f"SELECT DISTINCT ON (full_name) * FROM {company_id} WHERE CAST(date_added AS DATE) = CURRENT_DATE LIMIT 10;")


    with ENGINE.connect() as connection:
        result = connection.execute(query)
        column_names = result.keys()
    data = [dict(zip(column_names, row)) for row in result]

    for row in data:
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