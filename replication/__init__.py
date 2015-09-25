#!/usr/bin/env python
# encoding: utf-8
"""
replication/__init__.py

Created by Maximillian Dornseif on 2012-12-03.
Copyright (c) 2012, 2014 HUDORA GmbH. All rights reserved.
"""


# from https://developers.google.com/appengine/docs/python/datastore/metadataqueries#Python_Kind_queries

from google.appengine.ext.db.metadata import Kind

def get_all_datastore_kinds():
    # Start with unrestricted kind query
    q = Kind.all()

    # Print query results
    for k in q:
        if not k.kind_name.startswith('_'):
            yield k.kind_name
