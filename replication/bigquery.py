#!/usr/bin/env python
# encoding: utf-8
"""
replication/bigquery.py - Replikation von Cloud Datastore zu BigQuery.

Es werden Daten eines Datastore-Backups zu BigQuery exportiert.
Für Backups, s.
https://cloud.google.com/appengine/articles/scheduled_backups
https://github.com/mdornseif/appengine-toolkit/blob/master/gaetk/defaulthandlers.py

Extrahiert aus huWaWi
Created by Christian Klein on 2017-04-19.
Copyright (c) 2017 HUDORA. All rights reserved.
"""
import datetime
import logging
import re
import time

import cloudstorage
import webapp2

from google.appengine.api import lib_config
from google.appengine.api import taskqueue
from google.appengine.api.app_identity import get_application_id
from google.cloud import bigquery
from huTools.calendar.formats import convert_to_date
from webob.exc import HTTPServerError as HTTP500_ServerError


replication_config = lib_config.register(
    'gaetk_replication',
    dict(SQL_INSTANCE_NAME='*unset*',
         SQL_DATABASE_NAME='*unset*',
         SQL_QUEUE_NAME='default',
         MAXSIZE=1536 * 1024,
         MAXRECORDS=3000,
         BLACKLIST=[],
         BIGQUERY_PROJECT='project',
         BIGQUERY_DATASET='dataset',
         BIGQUERY_QUEUE_NAME='default',
         GS_BUCKET='bucketname')
)


def create_job(filename):
    u"""Erzeuge Job zum Upload einer Datastore-Backup-Datei zu Google BigQuery"""
    bigquery_client = bigquery.Client(project=replication_config.BIGQUERY_PROJECT)
    tablename = filename.split('.')[-2]
    resource = {
        'configuration': {
            'load': {
                'destinationTable': {
                    'projectId': replication_config.BIGQUERY_PROJECT,
                    'datasetId': replication_config.BIGQUERY_DATASET,
                    'tableId': tablename},
                'maxBadRecords': 0,
                'sourceUris': ['gs:/' + filename],
                'projectionFields': []
            }
        },
        'jobReference': {
            'projectId': 'huwawi2',
            'jobId': 'import-{}-{}'.format(tablename, int(time.time()))
        }
    }

    job = bigquery_client.job_from_resource(resource)
    job.source_format = 'DATASTORE_BACKUP'
    job.write_disposition = 'WRITE_TRUNCATE'
    return job


def upload_backup_file(filename):
    u"""Lade Datastore-Backup-Datei zu Google BigQuery"""
    job = create_job(filename)
    job.begin()

    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise HTTP500_ServerError(u'FAILED JOB, error_result: %s' % job.error_result)
            break
        time.sleep(5)  # ghetto polling


class CronReplication(webapp2.RequestHandler):
    """Steuerung der Replizierung zu Google BigQuery."""

    def get(self):
        u"""Regelmäßig von Cron aufzurufen."""
        bucketpath = '/'.join((replication_config.GS_BUCKET, get_application_id())) + '/'
        logging.info(u'searching backups in %r', bucketpath)

        objs = cloudstorage.listbucket(bucketpath, delimiter='/')
        subdirs = sorted((obj.filename for obj in objs if obj.is_dir), reverse=True)
        # Find Path of newest available backup
        # typical path:
        # '/appengine-backups-eu-nearline/hudoraexpress/2017-05-02/ag9...EM.ArtikelBild.backup_info'
        dirs = dict()
        for subdir in subdirs:
            datepart = subdir.rstrip('/').split('/')[-1]
            logging.debug(u'subdir: %s, datepart: %s', subdir, datepart)
            try:
                datum = convert_to_date(subdir.rstrip('/').split('/')[-1])
            except ValueError:
                continue
            else:
                dirs[datum] = subdir

        if not dirs:
            raise HTTP500_ServerError(u'No Datastore Backup found in %r' % bucketpath)

        datum = max(dirs)
        if datum < datetime.date.today() - datetime.timedelta(days=14):
            raise HTTP500_ServerError(u'Latest Datastore Backup in %r is way too old!' % bucketpath)
        subdir = dirs[datum]

        regexp = re.compile(subdir + r'(\w+)\.(\w+)\.backup_info')
        for obj in cloudstorage.listbucket(subdir):
            if regexp.match(obj.filename):
                taskqueue.add(
                    url=self.request.path,
                    params={'filename': obj.filename},
                    queue_name=replication_config.BIGQUERY_QUEUE_NAME)

    def post(self):
        filename = self.request.get('filename')
        logging.debug(u'uploading %s', filename)
        upload_backup_file(filename)


# for the python 2.7 runtime application needs to be top-level
application = webapp2.WSGIApplication([
    (r'^/gaetk_replication/bigquery/cron$', CronReplication),
], debug=True)
