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

from google.appengine.api import taskqueue
from google.appengine.api.app_identity import get_application_id
from google.cloud import bigquery
from huTools.calendar.formats import convert_to_date
from webob.exc import HTTPServerError as HTTP500_ServerError

import replication


def create_job(filename):
    u"""Erzeuge Job zum Upload einer Datastore-Backup-Datei zu Google BigQuery"""
    bigquery_client = bigquery.Client(project=replication.replication_config.BIGQUERY_PROJECT)
    tablename = filename.split('.')[-2]
    resource = {
        'configuration': {
            'load': {
                'destinationTable': {
                    'projectId': replication.replication_config.BIGQUERY_PROJECT,
                    'datasetId': replication.replication_config.BIGQUERY_DATASET,
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
    logging.info('uploading %r', filename)
    job = create_job(filename)
    job.begin()
    logging.info("job %s", job)

    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise HTTP500_ServerError(u'FAILED JOB, error_result: %s' % job.error_result)
        logging.debug("waiting %s %s", job.state, job)
        time.sleep(5)  # ghetto polling
    logging.debug("done %s", job)


class CronReplication(webapp2.RequestHandler):
    """Steuerung der Replizierung zu Google BigQuery."""

    def get(self):
        u"""Regelmäßig von Cron aufzurufen."""
        bucketpath = '/'.join((replication.replication_config.GS_BUCKET, get_application_id())) + '/'
        logging.info(u'searching backups in %r', bucketpath)

        subdirs = sorted((obj.filename for obj in
            cloudstorage.listbucket(
                bucketpath, delimiter='/') if obj.is_dir),
            reverse=True)

        # Find Path of newest available backup
        # typical path:
        # '/appengine-backups-eu-nearline/hudoraexpress/2017-05-02/ag9...EM.ArtikelBild.backup_info'
        regexp = re.compile(bucketpath + r'(\w+)\.(\w+)\.backup_info')
        datum, latestdatum, latestsubdir = None, None, None
        dirs = dict()
        for subdir in subdirs:
            logging.debug(u'subdir: %s', subdir)
            datepart = subdir.rstrip('/').split('/')[-1]
            logging.debug(u'datepart: %s', datepart)
            datum = None
            try:
                datum = convert_to_date(subdir.rstrip('/').split('/')[-1])
            except ValueError:
                continue
            dirs[datum] = subdir

        if not dirs:
            raise HTTP500_ServerError(u'No Datastore Backup found in %r' % bucketpath)

        datum = max(dirs)
        subdir = dirs[datum]

        if datum < datetime.date.today() - datetime.timedelta(days=14):
            raise HTTP500_ServerError(u'Latest Datastore Backup in %r is way too old!' % bucketpath)

        regexp = re.compile(subdir + r'(\w+)\.(\w+)\.backup_info')
        countdown = 1
        for obj in cloudstorage.listbucket(subdir):
            if regexp.match(obj.filename):
                taskqueue.add(
                    url=self.request.path,
                    params={'filename': obj.filename},
                    queue_name=replication.replication_config.BIGQUERY_QUEUE_NAME,
                    countdown=countdown)
                countdown += 2
        self.response.write('ok, countdown=%d\n' % countdown)

    def post(self):
        filename = self.request.get('filename')
        upload_backup_file(filename)
        self.response.write('ok\n')


# for the python 2.7 runtime application needs to be top-level
application = webapp2.WSGIApplication([
    (r'^/gaetk_replication/bigquery/cron$', CronReplication),
], debug=True)
