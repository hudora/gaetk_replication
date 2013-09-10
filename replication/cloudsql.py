#!/usr/bin/env python
# encoding: utf-8
"""
replication/cloudsql.py - Replikation von Daten in eine SQL Datenbank

Extraiert aus huWaWi
Created by Maximillian Dornseif on 2012-11-12.
Copyright (c) 2012, 2013 HUDORA. All rights reserved.
"""

import collections
import logging
import time
import sys
from datetime import datetime

import webapp2
from google.appengine.api import datastore, datastore_types
from google.appengine.api import lib_config
from google.appengine.api import rdbms
from google.appengine.api import taskqueue
from google.appengine.datastore import datastore_query
from google.appengine.ext.db import stats

# We want to avoid 'RequestTooLargeError' - the limit seems arround 1 MB
MAXSIZE = 900 * 1024

replication_config = lib_config.register('gaetk_replication',
    dict(SQL_INSTANCE_NAME='*unset*',
         SQL_DATABASE_NAME='*unset*',
         SQL_QUEUE_NAME='default',
         ))


def get_all_models():
    """Get list of all datastore models."""
    # Getting datastore statistics is slightly involved. We have to extract a
    # timestamp from `stats.GlobalStat.all().get()` and use that to access `stats.KindStat`:
    global_stat = stats.GlobalStat.all().get()
    if global_stat:
        timestamp = global_stat.timestamp
        ret = []
        for kindstat in stats.KindStat.all().filter("timestamp =", timestamp).fetch(200):
            if kindstat.kind_name and not kindstat.kind_name.startswith('__'):
                ret.append(kindstat.kind_name)
    return ret


def get_connetction():
    return rdbms.connect(instance=replication_config.SQL_INSTANCE_NAME,
                         database=replication_config.SQL_DATABASE_NAME)


class Table:
    """Keeps Infomation on a CloudSQL Table."""
    def __init__(self, kind):
        self.table_name = self.name = kind
        self.fields = dict(_key=str, updated_at=datetime)


def setup_table(kind):
    """Set-Up destination table."""
    table_name = kind
    conn = get_connetction()
    cur = conn.cursor()
    # retrieve table metadata if available
    cur.execute('SHOW tables LIKE "%s"' % table_name)
    if cur.fetchone():  # table exist
        # start with empty definition
        table = Table(kind)
        # add table fields
        cur.execute('SHOW COLUMNS FROM %s' % table_name)
        for col in cur.fetchall():
            field_name = col[0]
            field_type = col[1]
            table.fields[field_name] = field_type
    else:
        # self.table is missing
        table = Table(kind)
        sql = """CREATE TABLE %s (_key VARCHAR(255) NOT NULL,
                                _parent VARCHAR(255),
                                updated_at TIMESTAMP, PRIMARY KEY(_key))
               ENGINE MyISAM
               CHARACTER SET utf8 COLLATE utf8_general_ci""" % table_name
        logging.info(sql)
        cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Table setup for %s done", kind)
    return table


def get_type(value):
    """Datastore to Plain-Python Type Mapping"""
    ret = None
    if isinstance(value, datetime):
        ret = datetime
    elif isinstance(value, bool):
        ret = bool
    elif isinstance(value, long):
        ret = long
    elif isinstance(value, float):
        ret = float
    elif isinstance(value, int):
        ret = int
    elif isinstance(value, unicode):
        ret = unicode
    elif isinstance(value, str):
        ret = unicode
    elif isinstance(value, datastore_types.Text):
        ret = unicode
    elif isinstance(value, datastore_types.Key):
        ret = str
    #elif isinstance(value, datastore_types.Blob):
    #    return str
    #else:
    #    return str
    #return None
    if not ret:
        raise RuntimeError("unknown type %s %s" % (value, type(value)))
    return ret


def synchronize_field(table, field_name, field_type):
    """Ensure that the CloudSQL Table has all Columns we need."""
    if field_name not in table.fields:
        # table doesn't have this field yet - add it
        create_field(table.name, field_name, field_type)
        table.fields[field_name] = field_type


def create_field(table_name, field_name, field_type):
    """Create a Row in CloudSQL based on Datastore Datatype."""
    conn = get_connetction()
    cur = conn.cursor()
    if field_type == datetime:
        statement = "ALTER TABLE %s ADD COLUMN `%s` DATETIME" % (table_name, field_name)
    elif field_type in (int, long):
        statement = "ALTER TABLE %s ADD COLUMN `%s` BIGINT" % (table_name, field_name)
    elif field_type == float:
        statement = "ALTER TABLE %s ADD COLUMN `%s` FLOAT" % (table_name, field_name)
    elif field_type == bool:
        statement = "ALTER TABLE %s ADD COLUMN `%s` BOOLEAN" % (table_name, field_name)
    elif field_type in (str, unicode):
        statement = "ALTER TABLE %s ADD COLUMN `%s` VARCHAR(200)" % (table_name, field_name)
    else:
        raise RuntimeError("unknown field %s %s" % (field_name, field_type))

    try:
        cur.execute(statement)
    except rdbms.DatabaseError:
        logging.error(u'Error while executing statement %r', statement)
        raise

    conn.commit()
    cur.close()
    conn.close()


def normalize_entities(elist, table):
    """Ensure entitydicts all have the same keys in the same order."""
    new_entitydicts = []
    for entity in elist:
        edict = collections.OrderedDict()
        for name in table.fields.keys():
            edict[name] = entity.get(name, None)
        new_entitydicts.append(edict)
    return new_entitydicts


def get_listsize(l):
    """Recursive get the approximate Size of a list of list of strings."""
    siz = sys.getsizeof(l)
    if isinstance(l, list):
        for ele in l:
            if isinstance(ele, list):
                siz += get_listsize(ele)
            else:
                siz += sys.getsizeof(ele)
    return siz


def encode(value):
    """Encode value for database"""
    if isinstance(value, str):
        value = value.decode('utf-8', errors='replace')
    elif isinstance(value, list):
        if len(value):
            return encode(value[0])
        else:
            return None
    return value


def replicate(table, kind, cursor, stats):
    """Drive replication to Google CloudSQL."""
    batch_size = stats.get('batch_size', 10)
    start = time.time()

    if cursor:
        query = datastore.Query(kind=kind, cursor=cursor)
    else:
        query = datastore.Query(kind=kind)
    entitydicts = []
    for entity in query.Get(batch_size):
        parent = entity.key().parent()
        if not parent:
            parent = ''
        edict = collections.OrderedDict()
        for field, value in entity.items():
            value = encode(value)
            if value is not None:
                edict[field] = unicode(value)
        for field_name, field_value in edict.items():
            synchronize_field(table, field_name, get_type(field_value))
        edict.update(dict(_key=str(entity.key()), _parent=str(parent)))
        entitydicts.append(edict)

    entitydicts = normalize_entities(entitydicts, table)

    if not entitydicts:
        stats['time'] += time.time() - start
        return None
    else:
        conn = get_connetction()
        cur = conn.cursor()
        statement = 'REPLACE INTO `%s` (%s) VALUES (%s)' % (
            table.table_name,
            ','.join(('`%s`' % field for field in table.fields)),
            ','.join(['%s'] * len(table.fields.values())))

        thevalues = [x.values() for x in entitydicts]
        # We try a bigger batch next time
        if get_listsize(thevalues) < MAXSIZE / 2:
            stats['batch_size'] = batch_size * 2
            logging.info("incerasing batch_size to %d", stats['batch_size'])
        while thevalues and get_listsize(thevalues) > MAXSIZE:
            # Write in batches
            writelist = []
            writelist.append(thevalues.pop())
            if get_listsize(thevalues) > MAXSIZE:
                logging.debug("writing %d bytes", get_listsize(thevalues))
                cur.executemany(statement, writelist)
                writelist = []

        # write the rest
        if thevalues:
            logging.debug("writing %d bytes", get_listsize(thevalues))
            cur.executemany(statement, thevalues)
        logging.debug("alle %d geschrieben", len(entitydicts))
        conn.commit()
        cur.close()
        conn.close()
        stats['records'] += len(entitydicts)
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
                     batch_size=int(self.request.get('batch_size', 25)))
        if cursor:
            cursor = datastore_query.Cursor.from_websafe_string(cursor)
        table = setup_table(kind)
        cursor = replicate(table, kind, cursor, stats)

        logging.info(u'%s: bisher %d Records in %.1f s. Laufzeit %d s.',
                     kind, stats['records'], stats['time'],
                     time.time() - stats['starttime'])
        if cursor:
            params = dict(cursor=cursor.to_websafe_string(), kind=kind)
            params.update(stats)
            taskqueue.add(queue_name=replication_config.SQL_QUEUE_NAME,
                          url=self.request.path,
                          params=params)
        else:
            logging.info('%s fertig repliziert', kind)


class CronReplication(webapp2.RequestHandler):
    """Steuerung der Replizierung zu Google CloudSQL."""
    def get(self):
        """Wöchentlich von Cron aufzurufen."""
        for kind in get_all_models():
            taskqueue.add(queue_name=replication_config.SQL_QUEUE_NAME,
                          url='/gaetk_replication/cloudsql/worker',
                          params=dict(kind=kind))
        self.response.write('ok\n')


# for the python 2.7 runrime application needs to be top-level
application = webapp2.WSGIApplication([
    (r'^/gaetk_replication/cloudsql/worker$', TaskReplication),
    (r'^/gaetk_replication/cloudsql/cron$', CronReplication),
], debug=True)
