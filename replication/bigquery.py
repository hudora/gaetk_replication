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
import google.auth
import google_auth_httplib2
import httplib2
import webapp2

from google.appengine.api import taskqueue
from google.appengine.api.app_identity import get_application_id
from google.cloud import bigquery
from google.cloud.credentials import get_credentials
from huTools.calendar.formats import convert_to_date
from webob.exc import HTTPServerError as HTTP500_ServerError

from . import replication_config


def get_client():
    """BigQuery-Client erzeugen."""
    credentials = google.auth.credentials.with_scopes_if_required(get_credentials(), bigquery.Client.SCOPE)
    return bigquery.Client(
        project=replication_config.BIGQUERY_PROJECT,
        _http=google_auth_httplib2.AuthorizedHttp(credentials, httplib2.Http(timeout=60)))


def create_job(filename):
    u"""Erzeuge Job zum Upload einer Datastore-Backup-Datei zu Google BigQuery"""
    bigquery_client = get_client()
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
                'projectionFields': [],
                'source_format': 'DATASTORE_BACKUP',
                'write_disposition': 'WRITE_TRUNCATE',
            }
        },
        'jobReference': {
            'projectId': replication_config.BIGQUERY_PROJECT,
            'jobId': 'import-{}-{}'.format(tablename, int(time.time()))
        }
    }

    return bigquery_client.job_from_resource(resource)


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

        regexp = re.compile(subdir + r'(\w+)\.(\w+)\.backup_info')
        countdown = 1
        subdir = dirs[datum]
        logging.info(u'Uploading Backup %s from subdir', datum)
        regexp = re.compile(subdir + r'([\w-]+)\.(\w+)\.backup_info')
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


application = webapp2.WSGIApplication([
    (r'^/gaetk_replication/bigquery/cron$', CronReplication),
], debug=True)
