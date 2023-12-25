from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, Text
import io
from build_functions import *


def upload_to_azure_blob(blob_service_client, container_name, content, blob_name):
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    blob_client.upload_blob(content, overwrite=True)



def upload_profile_picture_blob(AZURE_ACCOUNT_URL,AZURE_CONNECTION_BLOB_STRING,AZURE_CONTAINER_NAME,image,user_id):
    blob_service_client = BlobServiceClient(account_url=AZURE_ACCOUNT_URL, credential=AZURE_CONNECTION_BLOB_STRING)
    blob_name = f'profile_images/{user_id}.png'
    upload_to_azure_blob(blob_service_client, AZURE_CONTAINER_NAME, io.BytesIO(image.read()), blob_name)



def delete_from_azure_blob(AZURE_ACCOUNT_URL,AZURE_CONNECTION_BLOB_STRING,container_name,company_id, blob_name):
    blob_service_client = BlobServiceClient(account_url=AZURE_ACCOUNT_URL, credential=AZURE_CONNECTION_BLOB_STRING)
    container_client = blob_service_client.get_container_client(container_name)

    print('aff')
    
    try:
        # List all blobs in the container
        blobs = container_client.walk_blobs(name_starts_with=f"{company_id}/")

        print(blobs)

        # Iterate through the blobs and delete those with the specified prefix
        for blob in blobs:
            print(blob)
            if blob_name in blob.name:
                blob_client = container_client.get_blob_client(blob.name)
                blob_client.delete_blob()
                print(f"Blob {blob.name} deleted successfully.")
    except Exception as e:
        print(f"Error deleting blobs {blob_name}: {str(e)}")


def create_df_file_from_db(AZURE_ACCOUNT_URL,AZURE_CONNECTION_BLOB_STRING,USERS_FILE_COLUMNS_ORDER,JOURNEY_FILE_COLUMNS_ORDER,ENGINE,AZURE_CONTAINER_NAME,company_id,segment_id = None,filters=None):
    
    blob_service_client = BlobServiceClient(account_url=AZURE_ACCOUNT_URL, credential=AZURE_CONNECTION_BLOB_STRING)

    # directory_path = f'/companies/{company_id}'
    file_types = ['users', 'journey']

    if segment_id == None:
        for file_type in file_types:
            file_name = f'main-{file_type}.csv'
            if file_type == 'users':
                column_names_str = ', '.join(USERS_FILE_COLUMNS_ORDER)
                query = text(f"SELECT DISTINCT ON (full_name) {column_names_str} FROM {company_id};")

                with ENGINE.connect() as connection:
                    data = connection.execute(query)
                
                df = pd.DataFrame(data, columns=USERS_FILE_COLUMNS_ORDER)
                
            if file_type == 'journey':
                column_names_str = ', '.join(JOURNEY_FILE_COLUMNS_ORDER)

                query = text(f"SELECT {column_names_str} FROM {company_id};")

                with ENGINE.connect() as connection:
                    data = connection.execute(query)
                
                df = pd.DataFrame(data, columns=JOURNEY_FILE_COLUMNS_ORDER)
                
            csv_content = df.to_csv(index=False)

            blob_name = f'{company_id}/{file_name}'
            upload_to_azure_blob(blob_service_client, AZURE_CONTAINER_NAME, io.BytesIO(csv_content.encode()), blob_name)

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


                with ENGINE.connect() as connection:
                    data = connection.execute(query)
                
                df = pd.DataFrame(data, columns=JOURNEY_FILE_COLUMNS_ORDER)
                
            csv_content = df.to_csv(index=False)

            blob_name = f'{company_id}/{file_name}'
            upload_to_azure_blob(blob_service_client, AZURE_CONTAINER_NAME, io.BytesIO(csv_content.encode()), blob_name)
            print('uploaded!')
    