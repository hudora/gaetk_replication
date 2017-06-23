#!/usr/bin/env python
# encoding: utf-8
"""
replication/__init__.py

Created by Maximillian Dornseif on 2012-12-03.
Copyright (c) 2012, 2014 HUDORA GmbH. All rights reserved.
"""
from google.appengine.api import lib_config
from google.appengine.ext.db.metadata import Kind


replication_config = lib_config.register(
    'gaetk_replication',
    dict(
        SQL_INSTANCE_NAME='*unset*',
        SQL_DATABASE_NAME='*unset*',
        SQL_QUEUE_NAME='default',
        MAXSIZE=1536 * 1024,
        MAXRECORDS=3000,
        BLACKLIST=[],
        BIGQUERY_PROJECT='project',
        BIGQUERY_DATASET='dataset',
        BIGQUERY_QUEUE_NAME='default',
        GS_BUCKET='bucketname'))


# from https://developers.google.com/appengine/docs/python/datastore/metadataqueries#Python_Kind_queries
def get_all_datastore_kinds():
    # Start with unrestricted kind query
    q = Kind.all()

    # Print query results
    for k in q:
        if not k.kind_name.startswith('_'):
            yield k.kind_name
