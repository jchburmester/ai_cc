import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect
import pandas as pd


class DatabaseHandler:
    def __init__(self, database_url):
        self.engine = sqlalchemy.create_engine(database_url)
        self.Session = sessionmaker(bind=self.engine)
        self.inspector = inspect(self.engine)

    def get_table_names(self):
        return self.inspector.get_table_names()

    def get_dataframe_from_table(self, table_name, column_names):
        session = self.Session()
        metadata = sqlalchemy.MetaData()
        metadata.reflect(bind=self.engine, views=True)
        table = metadata.tables[table_name]

        query = session.query(table).all()

        df = pd.DataFrame(query, columns=column_names)

        session.close()
        return df