import datetime
import logging
import json
import os

from kubernetes import client, config, watch
from peewee import BooleanField, CharField, DateTimeField, DoubleField, \
    SqliteDatabase, Model
import requests

# Some constants, may be moved to configuration
USERNAME_ANNOTATION = 'hub.jupyter.org/username'
DEFAULT_USERNAME = 'nobody'
DEFAULT_SPOOL_DIR = '/var/spool/egi-notebooks/'
DEFAULT_NAMESPACE = 'default'
DEFAULT_PROMETHEUS_URL = 'http://localhost:9000'
DEFAULT_NOTEBOOK_DB = 'notebooks.db'

# name of the environment variables where config is expected
NAMESPACE_ENV = 'NOTEBOOKS_NS'
SPOOL_DIR_ENV = 'SPOOL_DIR'
PROMETHEUS_URL_ENV = 'PROMETHEUS_URL'
NOTEBOOK_DB_ENV = 'NOTEBOOK_DB'

db = SqliteDatabase(None)

class Notebook(Model):
    uid = CharField()
    username = CharField()
    start = DateTimeField(null=True)
    end = DateTimeField(null=True)
    cpu_time = DoubleField(null=True)
    processed = BooleanField(default=False)

    class Meta:
        database = db

def get_usage_info(prometheus_url, namespace, pod_name):
    # TODO(enolfc): Making multiple queries here?
    query_str = ("container_cpu_usage_seconds_total{{namespace='{0}',"
                 "pod_name='{1}',container_name='notebook'}}")
    params = {'query': query_str.format(namespace, pod_name)}
    r = requests.get('{0}/api/v1/query'.format(prometheus_url), params=params)
    result = r.json()['data']['result']
    # take the last value
    return result[-1]['value'][1]


def process_event(event, namespace, prometheus_url, spool_dir):
    pod = event['object']
    username = pod.metadata.annotations.get(USERNAME_ANNOTATION,
                                            DEFAULT_USERNAME)
    logging.debug("Got %s for pod %s (user: %s)",
                  event['type'],
                  pod.medata.uid,
                  username)

    try:
        notebook = Notebook.get(Notebook.uid == pod.metadata.uid)
    except Notebook.DoesNotExist:
        notebook = Notebook.get(uid=pod.metadata.uid, username=username)

    if pod.status.container_statuses:
        state = pod.status.container_statuses[0].state
        if state.running:
            log.debug("Pod is running: %s", state.running)
            if state.running.started_at:
                notebook.start = state.running.started_at
            else:
                notebook.start = datetime.datetime.now()
        if state.terminated:
            log.debug("Pod is terminated: %s", state.terminated)
            if state.terminated.finished_at:
                notebook.end = state.terminated.finished_at
            else:
                notebook.end = datetime.datetime.now()
            # TODO(enolfc): Make multiple queries here
            notebook.cpu_time = get_usage_info(prometheus_url,
                                               namespace,
                                               pod.metadata.name)
    notebook.save()


def watch(namespace='', prometheus_url='', spool_dir=''):
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, namespace=namespace,
                          label_selector='component=singleuser-server'):
        process_event(event, namespace, prometheus_url, spool_dir)


def main():
    namespace = os.environ.get(NAMESPACE_ENV, DEFAULT_NAMESPACE)
    spool_dir = os.environ.get(SPOOL_DIR_ENV, DEFAULT_SPOOL_DIR)
    prometheus_url = os.environ.get(PROMETHEUS_URL_ENV, DEFAULT_PROMETHEUS_URL)
    db.init(os.environ.get(NOTEBOOK_DB_ENV, DEFAULT_NOTEBOOK_DB))
    db.connect()
    db.create_tables([Notebook])
    watch(namespace, prometheus_url, spool_dir)


if __name__ == '__main__':
    main()
