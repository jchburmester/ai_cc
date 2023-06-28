import logging
import os
from contextlib import AbstractContextManager
from distutils.util import strtobool
from typing import Optional

import sqlalchemy as sqla
from sqlalchemy import exc
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from dotenv import load_dotenv


class DBwrapper:
    def __init__(self, conf: dict = None):
        load_dotenv()
        self.id: int = 0

        conn_conf = os.environ
        if conf is not None:
            conn_conf = conf

        self.engine: sqla.engine.base.Engine = sqla.create_engine("postgresql+psycopg2://"
                                                                  + conn_conf["PG_USER"] + ":" + conn_conf["PG_PASSWORD"] + "@"
                                                                  + conn_conf["PG_HOST"] + ":" + conn_conf["PG_PORT"] + "/"
                                                                  + conn_conf["PG_DATABASE"], echo=strtobool(conn_conf.get("SQL_DEBUG", 'false')), pool_pre_ping=True)
        self.base_type = automap_base()
        self.base_type.prepare(self.engine, reflect=True)
        self._Session = sessionmaker(bind=self.engine)

    def get_table(self, table_name: str) -> type:
        return self.base_type.classes[table_name]

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