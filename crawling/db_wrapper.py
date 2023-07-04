'''This file is copied from github repo of @alek9z'''

import logging
import yaml
from contextlib import AbstractContextManager
from distutils.util import strtobool
from typing import Optional

import sqlalchemy as sqla
from sqlalchemy import exc
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from dotenv import load_dotenv
logging.getLogger('sqlalchemy').setLevel(logging.WARNING)


class DBwrapper:
    def __init__(self, conf: dict = None):
        load_dotenv()
        self.id: int = 0

        with open("config/db_config.yml", "r") as f:
            conf = yaml.safe_load(f)        

        db_string = f"postgresql+psycopg2://{conf['PG_USER']}:{conf['PG_PASSWORD']}@{conf['PG_HOST']}:{conf['PG_PORT']}/{conf['PG_DATABASE']}"
        self.engine: sqla.engine.base.Engine = sqla.create_engine(db_string, echo=False, pool_pre_ping=True)
        #self.engine: sqla.engine.base.Engine = sqla.create_engine(db_string, echo=conf['SQL_DEBUG'], pool_pre_ping=True)
        self.base_type = automap_base()
        self.base_type.prepare(self.engine, reflect=True)
        self._Session = sessionmaker(bind=self.engine)

    def get_table(self, table_name: str) -> sqla.Table:
        return self.base_type.metadata.tables[table_name]

    def create_session(self) -> Session:
        return self._Session()

    def remove_session(self) -> None:
        self._Session.remove()

    def set_scoped_session(self) -> None:
        self._Session = scoped_session(sessionmaker(bind=self.engine))


class SafeSession(AbstractContextManager):
    def __init__(self, db: DBwrapper, logger: Optional[logging.Logger]):
        self.__logger: Optional[logging.Logger] = logger
        self.__db: DBwrapper = db

    def __enter__(self) -> Session:
        self.session = self.__db.create_session()
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        # exp = None
        if exc_type is None and exc_val is None and exc_tb is None:
            try:
                self.session.commit()
            except exc.IntegrityError as e:
                self.session.rollback()
                if self.__logger is not None:
                    self.__logger.exception("Integrity error committing data")
            # Every other exception type will be raised
        else:
            self.session.rollback()
            if self.__logger is not None:
                self.__logger.error(
                    "Exception type: {}\nException exc value: {}\nTraceback: {}".format(exc_type, exc_val, exc_tb),
                    exc_info=exc_tb)
        self.session.close()
        # self.__db.remove_session() # TODO: needed?