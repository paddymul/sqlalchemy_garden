from sqlalchemy import Column, Integer, String, ForeignKey, Text, Table
from sqlalchemy import  Boolean, DateTime
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import configure_mappers
from lib.deferred_task import deferred


Base = declarative_base()

def bp(model_instance):
    return model_instance.initial_value < 5
        

class AfterInsertTable(Base):
    __tablename__ = 'after_insert_table'

    @deferred('after_insert', before_predicate=bp)
    def task(self, session):
        self.after_insert = self.initial_value + 10
        session.add(la)
        session.commit()

    id = Column(Integer, primary_key=True)
    initial_value = Column(Integer, default=0)
    after_insert = Column(Integer, default=0)

def update_bp(aut):
    return update_trigger > 3


class AfterUpdateTable(Base):
    __tablename__ = 'after_update_table'

    @deferred('after_update', change_field="update_trigger", 
              before_predicate=update_bp)
    def task(self, session):
        self.after_update = self.update_trigger + 10
        session.add(la)
        session.commit()

    id = Column(Integer, primary_key=True)
    update_trigger = Column(Integer, default=0)
    after_update = Column(Integer, default=0)



configure_mappers()
