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

    # TODO: Mit irgendwas schickem ersetzen? deferred oder taskqueue oder sowas?
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                logging.error(u'FAILED JOB, error_result: %s', job.error_result)
            break
        time.sleep(1)


class CronReplication(webapp2.RequestHandler):
    """Steuerung der Replizierung zu Google BigQuery."""

    def get(self):
        u"""Regelmäßig von Cron aufzurufen."""
        bucketname = '/' + '/'.join((replication_config.GS_BUCKET,get_application_id()))
        logging.info(u'searching backups in %r', bucketname)
        subdirs = sorted((obj.filename for obj in cloudstorage.listbucket(
            bucketname, delimiter='/') if obj.is_dir), reverse=True)

        datum = None
        latest = None
        for subdir in subdirs:
            logging.debug(u'subdir: %s', subdir)
            latest = subdir
            try:
                datum = convert_to_date(latest.rstrip('/').split('/')[-1])
            except ValueError:
                continue
            latest = subdir
            break

        if not datum:
            logging.error(u'No Datastore Backup found in %r', replication_config.GS_BUCKET)
            return
        elif datum < datetime.date.today() - datetime.timedelta(days=14):
            logging.error(u'Latest Datastore Backup is way too old!')
            return

        regexp = re.compile(latest + r'(\w+)\.(\w+)\.backup_info')
        for obj in cloudstorage.listbucket(latest):
            if regexp.match(obj.filename):
                taskqueue.add(
                    url=self.request.path,
                    params={'filename': obj.filename},
                    queue_name=replication_config.BIGQUERY_QUEUE_NAME)

    def post(self):
        filename = self.request.get('filename')
        upload_backup_file(filename)


# for the python 2.7 runtime application needs to be top-level
application = webapp2.WSGIApplication([
    (r'^/gaetk_replication/bigquery/cron$', CronReplication),
], debug=True)
