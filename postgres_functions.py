from sqlalchemy import create_engine, text, MetaData, Table, Column, Text


def create_postgres_table(table_name,COLUMNS_ORDER,ENGINE):
    metadata = MetaData()

    table = Table(
        table_name,
        metadata,
        *[Column(column, Text) for column in COLUMNS_ORDER]
    )

    metadata.create_all(ENGINE)
        
    return 'done'

def delete_postgres_table(table_name,ENGINE):
    metadata = MetaData()

    table = Table(table_name, metadata, autoload_with=ENGINE)

    table.drop(ENGINE)

