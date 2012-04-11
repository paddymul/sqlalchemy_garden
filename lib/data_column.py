from boto.s3.key import Key
from boto.s3.connection import S3Connection
import os
import hashlib
import sqlalchemy as sa
from sqlalchemy import (
            Column, ForeignKey, Table,
            Integer, String, create_engine
        )
from sqlalchemy.orm import (
            mapper, relationship,
            scoped_session, sessionmaker, configure_mappers
        )
from sqlalchemy.ext.declarative import declared_attr, declarative_base
from sqlalchemy import event
import re

class FileStorageEngine(object):
    def __init__(self, data_dir, base_url):
        self.data_dir = data_dir
        self.base_url = base_url
    def save(self, sha1sum, file_content):
        file_path = os.path.join(self.data_dir, sha1sum)
        open(file_path, "wb").write(file_content)

    def retrieve(self, sha1sum):
        file_path = os.path.join(self.data_dir, sha1sum)
        return open(file_path,"rb").read()

    def url(self, sha1sum):
        return self.base_url + "/static/FSE_dir/" + sha1sum


class S3StorageEngine(object):

    def __init__(self, bucket_name, access_key, secret_key):
        self.bucket_name, self.access_key = bucket_name, access_key
        self.secret_key = secret_key

    def save(self, sha1sum, file_content):
        conn = S3Connection(self.access_key, self.secret_key)
        b = conn.get_bucket(self.bucket_name)
        k = Key(b)
        k.key = sha1sum
        k.set_contents_from_string(file_content, policy='public-read')
        

    def retrieve(self, sha1sum):
        conn = S3Connection(self.access_key, self.secret_key)
        b = conn.get_bucket(self.bucket_name)
        k = Key(b)
        k.key = sha1sum
        return k.get_contents_as_string()
    
    def url(self, key):
        return "http://s3.amazonaws.com/%s/%s" % (self.bucket_name, key)


class DataColumn(sa.types.TypeDecorator):
    """  
    allows blob content to be stroed on the filesystem or s3
    transparently, only the sha1sum is stored to the database

    """
    impl=sa.types.VARCHAR

    def __init__(self, storage_engine, base_prop_name, *args, **kwargs):
        self.se = storage_engine
        self.base_prop_name = base_prop_name
        return super(DataColumn, self).__init__(*args, **kwargs)

    def save_val(self, file_content):
        # I would like to set self.sha1sum here, but I can't,
        # remember, only a single DataColumn is instatiated per class,
        # setting an attribute on self would conflict with other
        # instances of the parent table
        sha1sum = hashlib.sha1(file_content).hexdigest()
        self.se.save(sha1sum, file_content)
        return sha1sum

    def retrieve_val(self, sha1sum):
        print "calling retrieve_val for %s " % sha1sum
        if sha1sum:
            return self.se.retrieve(sha1sum)
        return None

    def url(self, sha1sum):
        return self.se.url(sha1sum)

class LinkedDataColumn(sa.types.TypeDecorator):
    """  
    Stores data to a file or s3, file is stored with a key of the
    value of another column.  This does not actually require a column
    in the database.

    """
    impl=sa.types.VARCHAR

    def __init__(self, storage_engine, base_prop_name, linked_column_name,
                 *args, **kwargs):
        self.se = storage_engine
        self.base_prop_name = base_prop_name
        
        return super(LinkedDataColumn, self).__init__(*args, **kwargs)

    def save_val(self, file_content):
        # I would like to set self.sha1sum here, but I can't,
        # remember, only a single DataColumn is instatiated per class,
        # setting an attribute on self would conflict with other
        # instances of the parent table
        sha1sum = hashlib.sha1(file_content).hexdigest()
        self.se.save(sha1sum, file_content)
        return sha1sum

    def retrieve_val(self, key):
        print "calling retrieve_val for %s " % sha1sum
        if sha1sum:
            return self.se.retrieve(sha1sum)
        return None

    def url(self, key):
        return self.se.url(sha1sum)



def monkey_class(model_class, col_name, data_column_instance):
    
    bn = data_column_instance.base_prop_name # base_name

    if not col_name == bn+"_sha1sum":
        print " When using a DataColumn it is recommended that the base column name be the"
        print " property name + _sha1sum.  You have %s and %s " % (bn, col_name)

    def base_getter(self):
        return data_column_instance.retrieve_val(
            getattr(self, col_name))
    
    def base_setter(self, value):
        sha1sum = data_column_instance.save_val(value)
        setattr(self, col_name, sha1sum)

    def url_getter(self):
        return data_column_instance.url(
            getattr(self, col_name))

    setattr(model_class, bn, property(base_getter, base_setter))
    setattr(model_class, bn+"_url", property(url_getter))

    
    

@event.listens_for(mapper, "mapper_configured")
def _setup_deferred_properties(mapper, class_):
    """Listen for finished mappers and apply DeferredProp
    configurations."""

    for col_name in mapper.local_table.c.keys():
        col = mapper.local_table.c.get(col_name)
        t = col.type
        
        if isinstance(t, DataColumn):
            monkey_class(class_, col_name, t)
