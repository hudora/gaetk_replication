#!/usr/bin/env python
# encoding: utf-8
"""
replication/cloudsql.py - Replikation von Daten in eine SQL Datenbank

Extraiert aus huWaWi
Created by Maximillian Dornseif on 2012-11-12.
Copyright (c) 2012, 2013, 2014, 2015 HUDORA. All rights reserved.
"""
import collections
import itertools
import logging
import sys
import time

from datetime import datetime

import google.appengine.ext.db
import replication
import webapp2

from google.appengine.api import datastore
from google.appengine.api import datastore_types
from google.appengine.api import lib_config
from google.appengine.api import rdbms
from google.appengine.api import taskqueue
from google.appengine.api import users
from google.appengine.datastore import datastore_query


# We want to avoid 'RequestTooLargeError' - the limit is 16 MB
MAXSIZE = 1024 * 1024
MAXRECORDS = 2500

replication_config = lib_config.register(
    'gaetk_replication',
    dict(SQL_INSTANCE_NAME='*unset*',
         SQL_DATABASE_NAME='*unset*',
         SQL_QUEUE_NAME='default',
         ))


def get_connection():
    """
    Create a connection to database

    The connection needs to be closed afterwards.
    """
    return rdbms.connect(
        instance=replication_config.SQL_INSTANCE_NAME, database=replication_config.SQL_DATABASE_NAME)


class DatabaseCursor(object):
    """Context Manager for connection to Google Cloud SQL"""
    def __enter__(self):
        self.connection = get_connection()
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exctype, value, traceback):
        if exctype is None:
            self.cursor.close()
            self.connection.commit()
        self.connection.close()


class Table(object):
    """Keeps Infomation on a CloudSQL Table."""
    def __init__(self, kind):
        self.name = kind
        self.fields = dict(_key=str, _parent=str, updated_at=datetime)

    def synchronize_field(self, field_name, value):
        """Add field to table"""
        if field_name not in self.fields:
            if value is not None:
                field_type = get_type(value)
                create_field(self.name, field_name, field_type)
                self.fields[field_name] = field_type

    def get_replace_statement(self):
        return 'REPLACE INTO `%s` (%s) VALUES (%s)' % (
            self.name,
            ','.join(('`%s`' % field for field in self.fields)),
            ','.join(['%s'] * len(self.fields)))

    def normalize_entities(self, entitylist, default=''):
        """Ensure all entities have all keys"""
        keys = self.fields.keys()
        return [[entity.get(name, default) for name in keys] for entity in entitylist]


def setup_table(kind):
    """Set-Up destination table."""

    table = Table(kind)

    with DatabaseCursor() as cursor:
        cursor.execute('SHOW tables LIKE "%s"' % kind)
        if cursor.fetchone():
            cursor.execute('SHOW COLUMNS FROM `%s`' % kind)
            for column in cursor.fetchall():
                field_name = column[0]
                table.fields[field_name] = column[1]
        else:
            statement = """CREATE TABLE `%s` (_key VARCHAR(255) BINARY NOT NULL PRIMARY KEY,
                                              _parent VARCHAR(255),
                                              updated_at TIMESTAMP)
                           ENGINE InnoDB
                           CHARACTER SET utf8 COLLATE utf8_general_ci""" % kind
            cursor.execute(statement)

    return table


def get_type(value):
    """Datastore to Plain-Python Type Mapping"""

    if isinstance(value, datetime):
        return datetime
    elif isinstance(value, (bool, long, int, float)):
        return type(value)
    elif isinstance(value, (str, unicode)):
        return unicode
    elif isinstance(value, datastore_types.Text):
        return unicode
    elif isinstance(value, list):
        return unicode
    elif isinstance(value, (datastore_types.Key, datastore_types.BlobKey)):
        return str
    elif isinstance(value, users.User):
        return str
    else:
        raise RuntimeError("unknown type %s %s" % (value, type(value)))


def create_field(table_name, field_name, field_type):
    """Create a Row in CloudSQL based on Datastore Datatype."""

    if field_type == datetime:
        statement = "ALTER TABLE `%s` ADD COLUMN `%s` DATETIME" % (table_name, field_name)
    elif field_type in (int, long):
        statement = "ALTER TABLE `%s` ADD COLUMN `%s` BIGINT" % (table_name, field_name)
    elif field_type == float:
        statement = "ALTER TABLE `%s` ADD COLUMN `%s` FLOAT" % (table_name, field_name)
    elif field_type == bool:
        statement = "ALTER TABLE `%s` ADD COLUMN `%s` BOOLEAN" % (table_name, field_name)
    elif field_type in (str, unicode):
        statement = "ALTER TABLE `%s` ADD COLUMN `%s` VARCHAR(200)" % (table_name, field_name)
    elif field_type == list:
        statement = "ALTER TABLE `%s` ADD COLUMN `%s` TEXT" % (table_name, field_name)
    else:
        raise RuntimeError("unknown field %s %s" % (field_name, field_type))

    with DatabaseCursor() as cursor:
        try:
            cursor.execute(statement)
            cursor.commit()
        except rdbms.DatabaseError:
            logging.error(u'Error while executing statement %r', statement)
            raise


def get_listsize(obj):
    """Recursive get the approximate size of a list of list of strings."""
    size = sys.getsizeof(obj)
    if isinstance(obj, list):
        for element in obj:
            if isinstance(element, list):
                size += get_listsize(element)
            else:
                size += sys.getsizeof(element)
    return size


def encode(value):
    """Encode value for database"""
    if isinstance(value, str):
        return value.decode('utf-8', errors='replace')
    elif isinstance(value, bool):
        return int(value)
    elif isinstance(value, list):
        return u'|'.join((unicode(encode(x)) for x in value))
    return value


def create_entity(entity):
    parent = entity.key().parent()
    if not parent:
        parent = ''
    return collections.OrderedDict(_key=str(entity.key()), _parent=str(parent))


def entity_list_generator(iterable, table):
    """Generator that yields entities as dicts."""
    try:
        for entity in iterable:
            edict = create_entity(entity)
            for field, value in entity.items():
                edict[field] = unicode(encode(value))
                table.synchronize_field(field, value)
            yield edict
    except google.appengine.ext.db.Timeout:
        logging.warning('datastore timeout')
        raise StopIteration
    except Exception, msg:
        logging.error(u'entity_list_generator: %r', msg, exc_info=True)
        raise StopIteration


def replicate(table, kind, cursor, stats, **kwargs):
    """Drive replication to Google CloudSQL."""

    start = time.time()

    if cursor:
        if isinstance(cursor, basestring):
            cursor = datastore_query.Cursor.from_websafe_string(cursor)
        query = datastore.Query(kind=kind, cursor=cursor)
    else:
        query = datastore.Query(kind=kind)

    if 'filters' in kwargs:
        for property_operator, value in kwargs['filters']:
            query[property_operator] = value

    batch_size = stats.get('batch_size', 10)
    query_iterator = query.Run(limit=batch_size, offset=0)

    entitydicts = entity_list_generator(query_iterator, table)
    entities = table.normalize_entities(entitydicts)

    if not entities:
        stats['time'] += time.time() - start
        return None

    # MAXSIZE is chosen very conservativly.
    # Even if a batch is larger, it's very likely not too large
    # for a single write call.
    try:
        with DatabaseCursor() as cursor:
            cursor.executemany(table.get_replace_statement(), entities)
            stats['records'] += cursor.rowcount
    except (rdbms.InternalError, rdbms.IntegrityError), msg:
        logging.warning(u'Caught RDBMS exception: %s', msg)
    except TypeError as exception:
        if 'not enough arguments' in str(exception):
            logging.debug(u'statement: %r', table.get_replace_statement(), exc_info=True)
            logging.debug(u'%d: %r', len(table.fields), table.fields)
            for entity in entities:
                logging.debug(u'(%d): %s', len(entity), entity)
                break
        raise
    except:
        logging.debug(u'statement: %r', table.get_replace_statement(), exc_info=True)
        raise

    # Adapt batch size. This could be further optimized in the future,
    # like adapting it to a ratio of size and MAXSIZE.
    size = get_listsize(entities)
    if size * 2 < MAXSIZE:
        stats['batch_size'] = int(min([MAXRECORDS, batch_size * 2]))
        logging.info(u'increasing batch_size to %d', stats['batch_size'])
    elif size > MAXSIZE:
        stats['batch_size'] = int(min([MAXRECORDS, batch_size * 0.8]))
        logging.info(u'decreasing batch_size to %d', stats['batch_size'])

    stats['time'] += time.time() - start
    return query.GetCursor()


class TaskReplication(webapp2.RequestHandler):
    """Replicate a single Model to CloudSQL."""
    def get(self):
        """Start Task manually."""
        kind = self.request.get('kind')
        taskqueue.add(queue_name=replication_config.SQL_QUEUE_NAME,
                      url=self.request.path,
                      params=dict(kind=kind))
        self.response.write('ok\n')

    def post(self):
        """Is called for each model and then chains to itself"""
        kind = self.request.get('kind')
        cursor = self.request.get('cursor', None)
        stats = dict(records=int(self.request.get('records', 0)),
                     time=float(self.request.get('time', 0)),
                     starttime=int(self.request.get('starttime', time.time())),
                     batch_size=int(float(self.request.get('batch_size', 25))))
        if cursor:
            cursor = datastore_query.Cursor.from_websafe_string(cursor)
        table = setup_table(kind)
        logging.info(u'%s: starte %d Batch', kind, stats['batch_size'])
        cursor = replicate(table, kind, cursor, stats)
        logging.info(u'%s: bisher %d Records in %.1f s. Laufzeit %d s.',
                     kind, stats['records'], stats['time'],
                     time.time() - stats['starttime'])
        if cursor:
            params = dict(cursor=cursor.to_websafe_string(), kind=kind)
            params.update(stats)
            taskqueue.add(queue_name=replication_config.SQL_QUEUE_NAME,
                          name='%s-%s-%s' % (kind, stats['records'], int(time.time())),
                          url=self.request.path,
                          params=params)
        else:
            logging.info('%s fertig repliziert', kind)


class CronReplication(webapp2.RequestHandler):
    """Steuerung der Replizierung zu Google CloudSQL."""
    def get(self):
        """Wöchentlich von Cron aufzurufen."""

        models = self.request.get_all('kind')
        if not models:
            models = replication.get_all_datastore_kinds()

        for index, kind in enumerate(models):
            if kind.startswith('_'):
                continue
            if kind.startswith('ic_'):
                countdown = 1
            elif kind.startswith('fk_'):
                countdown = 5
            elif kind.startswith('bi_'):
                countdown = 60
            else:
                countdown = 300
            taskqueue.add(queue_name=replication_config.SQL_QUEUE_NAME,
                          url='/gaetk_replication/cloudsql/worker/%s' % kind,
                          params=dict(kind=kind),
                          name='%s-%s' % (kind, int(time.time())),
                          countdown=index * countdown)
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.write('ok\n')


# for the python 2.7 runtime application needs to be top-level
application = webapp2.WSGIApplication([
    (r'^/gaetk_replication/cloudsql/worker.*$', TaskReplication),
    (r'^/gaetk_replication/cloudsql/cron$', CronReplication),
], debug=True)
