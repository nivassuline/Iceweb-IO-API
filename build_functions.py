from sqlalchemy import create_engine, text, MetaData, Table, Column, Text
from datetime import datetime



def build_date_query(start_date = None, end_date = None, search=None):
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


    # Create a query to find documents within the specified date range
    date_range_query = text(f"""
        CAST(date AS DATE) >= '{start_date_str}' AND CAST(date AS DATE) <= '{end_date_str}'
    """)

    return date_range_query

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
     


def build_df_date_query(df, start_date, end_date, search=None):
    # Assume df is your Pandas DataFrame containing the 'date' column
    if start_date in ['undefined', None]:
        start_datetime_to_obj = datetime.today()
        start_datetime = start_datetime_to_obj.strftime('%Y-%m-%d')
        end_datetime_to_obj = datetime.strptime(
            f'{datetime.now().year}-01-01', '%Y-%m-%d')
        end_datetime = end_datetime_to_obj.strftime('%Y-%m-%d')

        # Create a boolean mask for date range query
        date_range_mask = (df['date'] >= end_datetime) & (
            df['date'] <= start_datetime)
    else:
        # Parse the start_date and end_date as datetime objects
        start_datetime_to_obj = datetime.strptime(start_date, '%Y-%m-%d')
        start_datetime = start_datetime_to_obj.strftime('%Y-%m-%d')
        end_datetime_to_obj = datetime.strptime(end_date, '%Y-%m-%d')
        end_datetime = end_datetime_to_obj.strftime('%Y-%m-%d')

        # Create a boolean mask for date range query
        date_range_mask = (df['date'] >= start_datetime) & (
            df['date'] <= end_datetime)

    # Apply the mask to filter rows in the DataFrame
    filtered_df = df.loc[date_range_mask]

    return filtered_df