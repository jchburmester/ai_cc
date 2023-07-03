import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

engine = sqlalchemy.create_engine('postgresql://postgres:ai_cc_23@localhost:5432/scopus_data')

Session = sessionmaker(bind=engine)
session = Session()

Base = automap_base()
Base.prepare(engine, reflect=True)

scopus_data = Base.classes.scopus_data

query = session.query(scopus_data).all()

print(query)

session.close()
