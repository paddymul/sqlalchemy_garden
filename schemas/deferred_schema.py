import pdb
import unittest
from sqlalchemy import Column, Integer, String, ForeignKey, Text, Table
from sqlalchemy import  Boolean, DateTime
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import configure_mappers
from lib.deferred_task import deferred
from conf import config
from lib.db_connection import db_session

Base = declarative_base()

def update_bp(aut):
    return aut.update_trigger > 3


class AfterUpdateTable(Base):
    __tablename__ = 'after_update_table'

    @deferred('after_update', change_field="update_trigger", 
              before_predicate=update_bp)
    def task(self, session):
        self.after_update = self.update_trigger + 10
        session.add(self)
        session.commit()

    id = Column(Integer, primary_key=True)
    update_trigger = Column(Integer, default=0)
    after_update = Column(Integer, default=0)


def bp(model_instance):
    print "bp"
    print model_instance.initial_value
    return model_instance.initial_value < 5
        

class AfterInsertTable(Base):
    __tablename__ = 'after_insert_table'

    @deferred('after_insert', before_predicate=bp)
    def task(self, session):
        print "task"
        self.after_insert = self.initial_value + 10
        session.add(self)
        session.commit()

    id = Column(Integer, primary_key=True)
    initial_value = Column(Integer, default=0)
    after_insert = Column(Integer, default=0)

configure_mappers()


class DeferredSchemaTestCase(unittest.TestCase):
    def test_after_insert(self):

        with db_session(config.DB_URI) as ses:
            Base.metadata.create_all(ses.bind)
            ses.add(AfterInsertTable(initial_value=3))
            ses.commit()
            aut1 = ses.query(AfterInsertTable).all()[0]
            self.assertEquals(aut1.after_insert, 13)

    def t_est_after_update(self):
        with db_session(config.DB_URI) as ses:
            ses.add(AfterUpdateTable(update_trigger=3))
            ses.commit()
            aut1 = ses.query(AfterUpdateTable).all()[0]
            self.assertEquals(aut1.after_update, 0)
            aut1.update_trigger = 4
            ses.add(aut1)
            ses.commit()

            aut2 = ses.query(AfterUpdateTable).all()[0]
            self.assertEquals(aut2.after_update, 14)

            


    
if __name__ == '__main__':
    '''
    with db_session(config.DB_URI) as ses:
        Base.metadata.create_all(ses.bind)
        '''
    unittest.main()


