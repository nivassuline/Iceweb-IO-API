from sqlalchemy import create_engine, text, MetaData, Table, Column, Text
import re
from psycopg2 import pool, sql
from app_functions import *


def get_most_popular(ENGINE, table_name, date_range_query, popular_type, state=None, filter_query=None):
    item_list = []
    count_list = []

    if filter_query is not None:
        with ENGINE.connect() as connection:
            if popular_type == 'hour':
                query = text(f"""
                    SELECT substr(hour, 1, 2) as hour_part, COUNT(*) as count
                    FROM {table_name}
                    WHERE ({date_range_query} AND {filter_query})
                    GROUP BY hour_part
                    ORDER BY count DESC
                    LIMIT 10
                """)
                result = connection.execute(query)

                for row in result:
                    item_list.append(convert_to_user_friendly_time(row[0]))
                    count_list.append(row[1])

            elif popular_type == 'state':
                query = text(f"""
                    SELECT {popular_type} as item, COUNT(*) as count
                    FROM {table_name}
                    WHERE ({date_range_query} AND {filter_query})
                    GROUP BY {popular_type}
                    ORDER BY count DESC
                    """)

                result = connection.execute(query)

                for row in result:
                    item_list.append(row[0])
                    count_list.append(row[1])
            elif popular_type == 'city':
                query = text(f"""
                    SELECT {popular_type} as item, COUNT(*) as count
                    FROM {table_name}
                    WHERE ({date_range_query} AND {filter_query} AND state = '{state}')
                    GROUP BY {popular_type}
                    ORDER BY count DESC
                    """)

                result = connection.execute(query)

                for row in result:
                    item_list.append(row[0])
                    count_list.append(row[1])
            else:
                query = text(f"""
                    SELECT {popular_type} as item, COUNT(*) as count
                    FROM {table_name}
                    WHERE ({date_range_query} AND {filter_query})
                    GROUP BY {popular_type}
                    ORDER BY count DESC
                    LIMIT 10
                """)

                result = connection.execute(query)

                total = 0
                count_list_new = []
                if popular_type == 'income_levels':
                    for row in result:
                        if row[0] == '-' or 'Unknown' in row[0]:
                            pass
                        else:
                            numbers = re.findall(r'\d+', row[0])
                            if '150' in numbers and len(numbers) == 1:
                                item_list.append('Greater Then 150K')
                            elif '30' in numbers and len(numbers) == 1:
                                item_list.append('Less Then 30K')
                            elif len(numbers) == 2:
                                item_list.append(
                                    f'{numbers[0]}K-{numbers[1]}K')
                            total += int(row[1])
                            count_list_new.append(row[1])
                    for count in count_list_new:
                        count_list.append(calculate_percentage(count, total))
                else:
                    for row in result:
                        item_list.append(row[0])
                        count_list.append(row[1])
    else:
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
                    item_list.append(convert_to_user_friendly_time(row[0]))
                    count_list.append(row[1])

            elif popular_type == 'state':
                query = text(f"""
                    SELECT {popular_type} as item, COUNT(*) as count
                    FROM {table_name}
                    WHERE {date_range_query}
                    GROUP BY {popular_type}
                    ORDER BY count DESC
                    """)

                result = connection.execute(query)

                for row in result:
                    item_list.append(row[0])
                    count_list.append(row[1])
            elif popular_type == 'city':
                query = text(f"""
                    SELECT {popular_type} as item, COUNT(*) as count
                    FROM {table_name}
                    WHERE ({date_range_query} AND state = '{state}')
                    GROUP BY {popular_type}
                    ORDER BY count DESC
                    """)

                result = connection.execute(query)

                for row in result:
                    item_list.append(row[0])
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

                total = 0
                count_list_new = []
                if popular_type == 'income_levels':
                    for row in result:
                        if row[0] == '-' or 'Unknown' in row[0]:
                            pass
                        else:
                            numbers = re.findall(r'\d+', row[0])
                            if '150' in numbers and len(numbers) == 1:
                                item_list.append('Greater Then 150K')
                            elif '30' in numbers and len(numbers) == 1:
                                item_list.append('Less Then 30K')
                            elif len(numbers) == 2:
                                item_list.append(
                                    f'{numbers[0]}K-{numbers[1]}K')
                            total += int(row[1])
                            count_list_new.append(row[1])
                    for count in count_list_new:
                        count_list.append(calculate_percentage(count, total))
                else:
                    for row in result:
                        item_list.append(row[0])
                        count_list.append(row[1])

    return item_list, count_list


def get_by_precent_count(ENGINE, table_name, field, date_range_query, filter_query=None):
    item_list = []
    count_list = []

    if filter_query is not None:
        with ENGINE.connect() as connection:
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
                        {table_name}
                    WHERE ({date_range_query} AND {filter_query}) -- Replace with your actual date range condition
                    GROUP BY
                        age_range
                    ORDER BY
                        count DESC
                    LIMIT 10;

                """)
            else:
                query = text(f"""
                    SELECT {field} as item, COUNT(DISTINCT full_name) AS count 
                    FROM {table_name}
                    WHERE ({date_range_query} AND {filter_query})
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
    else:
        with ENGINE.connect() as connection:
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
                        {table_name}
                    WHERE
                        {date_range_query} -- Replace with your actual date range condition
                    GROUP BY
                        age_range
                    ORDER BY
                        count DESC
                    LIMIT 10;
                """)
            else:
                query = text(f"""
                    SELECT {field} as item, COUNT(DISTINCT full_name) as count 
                    FROM {table_name}
                    WHERE {date_range_query}
                    GROUP BY {field}
                    ORDER BY count DESC
                    LIMIT 10;
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
    return item_list, count_list


def get_counts(ENGINE, table_name, date_range_query, filter_query=None):
    with ENGINE.connect() as connection:
        if filter_query is not None:
            query = text(f"""
                SELECT COUNT(DISTINCT "full_name") as total_distinct_count
                FROM {table_name}
                WHERE ({date_range_query} AND {filter_query})
            """)
            result = connection.execute(query).fetchall()
            journey_count = connection.execute(
                text(f"SELECT COUNT(*) FROM {table_name} WHERE {date_range_query} AND {filter_query}")).scalar()
            try:
                # Assuming "full_name" is the first column in the SELECT statement
                people_count = result[0][0]
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
                # Assuming "fullName" is the first column in the SELECT statement
                people_count = result_people[0][0]
                # Assuming "count" is the first column in the SELECT statement
                journey_count = result_journey[0][0]
            except IndexError:
                people_count = 0
                journey_count = 0

    return journey_count, people_count


def get_average(ENGINE, value, company_id, date_range_query,filter_query=None):
    with ENGINE.connect() as connection:
        if filter_query is not None:
            if value == 'age' or value == 'mortgage_age':
                query = text(f"""
                    SELECT AVG(CAST(NULLIF({value},'-') AS FLOAT))
                    FROM {company_id}
                    WHERE ({value} NOT LIKE '-' AND {value} IS NOT NULL AND {filter_query})
                """)

                result = connection.execute(query).scalar()

                try:
                    return round(result, 1)
                except TypeError:
                    return 0
            if value == 'net_worth':
                query = text(f"""
                    SELECT AVG(
                            CASE
                                WHEN {value} ~* 'J\. Greater than \$499,999' THEN 500000
                                WHEN {value} ~* 'B\. Less than \$1' THEN 1
                                WHEN {value} ~* '^[A\-\.]' THEN NULL
                                ELSE (
                                    (
                                        SELECT AVG(
                                            (CAST(replace(substring({value} FROM '\$([\d,]+) - \$([\d,]+)'), ',', '') AS NUMERIC) +
                                            CAST(replace(substring({value} FROM '\$([\d,]+) - \$([\d,]+)'), ',', '') AS NUMERIC)) / 2.0
                                        )
                                        FROM {company_id}
                                        WHERE {value} ~ '\$([\d,]+) - \$([\d,]+)'
                                    )::NUMERIC
                                )
                            END
                        ) AS average_{value}
                        FROM {company_id}
                        WHERE ({value} !~* '^[A\-\.]' AND {date_range_query} AND {filter_query});
                    """)

                result = connection.execute(query).scalar()

                try:
                    return f'${format(round(result),",")}'
                except TypeError:
                    return 0
            if value == 'household_income':
                query = text(f"""
                    SELECT AVG(
                            CASE
                                WHEN {value} ~* 'O\. \$250K +' THEN 250000
                                WHEN {value} ~* 'A\. Under \$10,000' THEN 10000
                                WHEN {value} ~* '^[A\-\.]' THEN NULL
                                ELSE (
                                    (
                                        SELECT AVG(
                                            (CAST(replace(substring({value} FROM '\$([\d,]+)-\$([\d,]+)'), ',', '') AS NUMERIC) +
                                            CAST(replace(substring({value} FROM '\$([\d,]+)-\$([\d,]+)'), ',', '') AS NUMERIC)) / 2.0
                                        )
                                        FROM {company_id}
                                        WHERE {value} ~ '\$([\d,]+)-\$([\d,]+)'
                                    )::NUMERIC
                                )
                            END
                        ) AS average_{value}
                        FROM {company_id}
                        WHERE({value} !~* '^[A\-\.]' AND {date_range_query} AND {filter_query});
                    """)

                result = connection.execute(query).scalar()
                try:
                    return f'${format(round(result),",")}'
                except TypeError:
                    return 0

            if value == 'credit_range':
                query = text(f"""
                SELECT AVG(
                        CASE
                            WHEN {value} ~* 'A\. 800+' THEN 850
                            WHEN {value} ~* 'H\. 499 \& Less' THEN 300
                            WHEN {value} ~* '^[A\-\.]' THEN NULL
                            ELSE
                                    (
                                        SELECT AVG(
                                            (CAST(substring({value} FROM '(\d+)-(\d+)') AS NUMERIC) +
                                            CAST(substring({value} FROM '(\d+)-(\d+)') AS NUMERIC)) / 2.0
                                        )
                                        FROM {company_id}
                                        WHERE {value} ~ '(\d+)-(\d+)'
                                    )::NUMERIC
                        END
                    ) AS average_{value}
                    FROM {company_id}
                    WHERE ({value} !~* '^[A\-\.]' AND {date_range_query} AND {filter_query});
                    """)

                result = connection.execute(query).scalar()

                try:
                    return round(result)
                except TypeError:
                    return 0
            if value == 'income_levels':
                query = text(f"""
                SELECT AVG(
                        CASE
                            WHEN {value} ~* 'GT_150K' THEN 250
                            WHEN {value} ~* 'LT_30K' THEN 1
                            WHEN {value} ~* '100K_150' THEN 125
                            WHEN {value} ~* '^[A\-\.]' THEN NULL
                            ELSE
                                    (
                                        SELECT AVG(
                                            (CAST(substring({value} FROM '(\d+)K_(\d+)K') AS NUMERIC) +
                                            CAST(substring({value} FROM '(\d+)K_(\d+)K') AS NUMERIC)) / 2.0
                                        )
                                        FROM {company_id}
                                        WHERE {value} ~ '(\d+)K_(\d+)K'
                                    )::NUMERIC
                        END
                    ) AS average_{value}
                    FROM {company_id}
                    WHERE ({value} !~* '^[A\-\.]' AND {date_range_query} AND {filter_query});
                    """)

                result = connection.execute(query).scalar()

                try:
                    return f'${format(round(result) * 1000,",")}'
                except TypeError:
                    return 0

            if value == 'home_price' or value == 'home_value' or value == 'mortgage_amount':
                query = text(f"""
                    SELECT AVG(CAST(NULLIF({value},'-') AS FLOAT))
                    FROM {company_id}
                    WHERE ({value} NOT LIKE '-' AND {value} IS NOT NULL AND {date_range_query} AND {filter_query})
                """)

                result = connection.execute(query).scalar()

                try:
                    return f'${format(round(result),",")}'
                except TypeError:
                    return 0
        else:
            if value == 'age' or value == 'mortgage_age':

                query = text(f"""
                    SELECT AVG(CAST(NULLIF({value},'-') AS FLOAT))
                    FROM {company_id}
                    WHERE {value} NOT LIKE '-' AND {value} IS NOT NULL
                """)

                result = connection.execute(query).scalar()

                try:
                    return round(result, 1)
                except TypeError:
                    return 0
            if value == 'net_worth':
                query = text(f"""
                    SELECT AVG(
                            CASE
                                WHEN {value} ~* 'J\. Greater than \$499,999' THEN 500000
                                WHEN {value} ~* 'B\. Less than \$1' THEN 1
                                WHEN {value} ~* '^[A\-\.]' THEN NULL
                                ELSE (
                                    (
                                        SELECT AVG(
                                            (CAST(replace(substring({value} FROM '\$([\d,]+) - \$([\d,]+)'), ',', '') AS NUMERIC) +
                                            CAST(replace(substring({value} FROM '\$([\d,]+) - \$([\d,]+)'), ',', '') AS NUMERIC)) / 2.0
                                        )
                                        FROM {company_id}
                                        WHERE {value} ~ '\$([\d,]+) - \$([\d,]+)'
                                    )::NUMERIC
                                )
                            END
                        ) AS average_{value}
                        FROM {company_id}
                        WHERE ({value} !~* '^[A\-\.]' AND {date_range_query});
                    """)

                result = connection.execute(query).scalar()

                try:
                    return f'${format(round(result),",")}'
                except TypeError:
                    return 0
            if value == 'household_income':
                query = text(f"""
                    SELECT AVG(
                            CASE
                                WHEN {value} ~* 'O\. \$250K +' THEN 250000
                                WHEN {value} ~* 'A\. Under \$10,000' THEN 10000
                                WHEN {value} ~* '^[A\-\.]' THEN NULL
                                ELSE (
                                    (
                                        SELECT AVG(
                                            (CAST(replace(substring({value} FROM '\$([\d,]+)-\$([\d,]+)'), ',', '') AS NUMERIC) +
                                            CAST(replace(substring({value} FROM '\$([\d,]+)-\$([\d,]+)'), ',', '') AS NUMERIC)) / 2.0
                                        )
                                        FROM {company_id}
                                        WHERE {value} ~ '\$([\d,]+)-\$([\d,]+)'
                                    )::NUMERIC
                                )
                            END
                        ) AS average_{value}
                        FROM {company_id}
                        WHERE({value} !~* '^[A\-\.]' AND {date_range_query});
                    """)

                result = connection.execute(query).scalar()
                try:
                    return f'${format(round(result),",")}'
                except TypeError:
                    return 0

            if value == 'credit_range':
                query = text(f"""
                SELECT AVG(
                        CASE
                            WHEN {value} ~* 'A\. 800+' THEN 850
                            WHEN {value} ~* 'H\. 499 \& Less' THEN 300
                            WHEN {value} ~* '^[A\-\.]' THEN NULL
                            ELSE
                                    (
                                        SELECT AVG(
                                            (CAST(substring({value} FROM '(\d+)-(\d+)') AS NUMERIC) +
                                            CAST(substring({value} FROM '(\d+)-(\d+)') AS NUMERIC)) / 2.0
                                        )
                                        FROM {company_id}
                                        WHERE {value} ~ '(\d+)-(\d+)'
                                    )::NUMERIC
                        END
                    ) AS average_{value}
                    FROM {company_id}
                    WHERE ({value} !~* '^[A\-\.]' AND {date_range_query});
                    """)

                result = connection.execute(query).scalar()

                try:
                    return round(result)
                except TypeError:
                    return 0
            if value == 'income_levels':
                query = text(f"""
                SELECT AVG(
                        CASE
                            WHEN {value} ~* 'GT_150K' THEN 250
                            WHEN {value} ~* 'LT_30K' THEN 1
                            WHEN {value} ~* '100K_150' THEN 125
                            WHEN {value} ~* '^[A\-\.]' THEN NULL
                            ELSE
                                    (
                                        SELECT AVG(
                                            (CAST(substring({value} FROM '(\d+)K_(\d+)K') AS NUMERIC) +
                                            CAST(substring({value} FROM '(\d+)K_(\d+)K') AS NUMERIC)) / 2.0
                                        )
                                        FROM {company_id}
                                        WHERE {value} ~ '(\d+)K_(\d+)K'
                                    )::NUMERIC
                        END
                    ) AS average_{value}
                    FROM {company_id}
                    WHERE ({value} !~* '^[A\-\.]' AND {date_range_query});
                    """)

                result = connection.execute(query).scalar()

                try:
                    return f'${format(round(result) * 1000,",")}'
                except TypeError:
                    return 0

            if value == 'home_price' or value == 'home_value' or value == 'mortgage_amount':
                query = text(f"""
                    SELECT AVG(CAST(NULLIF({value},'-') AS FLOAT))
                    FROM {company_id}
                    WHERE {value} NOT LIKE '-' AND {value} IS NOT NULL AND {date_range_query}
                """)

                result = connection.execute(query).scalar()

                try:
                    return f'${format(round(result),",")}'
                except TypeError:
                    return 0

