
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base


class db_session:
    engines = {}

    @staticmethod 
    def get_engine(database_uri):
        if not database_uri in db_session.engines:
            db_session.engines[database_uri] = sa.create_engine(
                database_uri, pool_size=10, max_overflow=0, echo=False)
        return db_session.engines[database_uri]

    def __init__(self, database_uri):
        Base = declarative_base()
        Base.metadata.create_all(self.get_engine(database_uri))
        Session = sa.orm.sessionmaker(bind=self.get_engine(database_uri))
        self.s = Session()

    def __enter__(self):
        return self.s
    
    def __exit__(self, type, value, traceback):
        self.s.close()

    def unit_test():
        assert False

