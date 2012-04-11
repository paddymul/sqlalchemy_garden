
def get_mem_engine():
    return create_engine('sqlite:///foo.db')

def get_mem_session():
    Base = declarative_base()
    Base.metadata.create_all(self.get_engine(database_uri))
    Session = sa.orm.sessionmaker(bind=self.get_engine(database_uri))

