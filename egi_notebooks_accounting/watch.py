import datetime
import logging
import json
import os

import kubernetes
import peewee
import requests

# Some constants, may be moved to configuration
USERNAME_ANNOTATION = 'hub.jupyter.org/username'
DEFAULT_USERNAME = 'nobody'
DEFAULT_NAMESPACE = 'default'
DEFAULT_PROMETHEUS_URL = 'http://localhost:9000'
DEFAULT_NOTEBOOK_DB = 'notebooks.db'

# name of the environment variables where config is expected
NAMESPACE_ENV = 'NOTEBOOKS_NS'
PROMETHEUS_URL_ENV = 'PROMETHEUS_URL'
NOTEBOOK_DB_ENV = 'NOTEBOOK_DB'

db = peewee.SqliteDatabase(None)

class Notebook(peewee.Model):
    uid = peewee.CharField()
    username = peewee.CharField()
    start = peewee.DateTimeField(null=True)
    end = peewee.DateTimeField(null=True)
    cpu_time = peewee.DoubleField(null=True)
    processed = peewee.BooleanField(default=False)

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
    try:
        return result[-1]['value'][1]
    except IndexError:
        # oops, what happened?
        logging.error('Unexpected exception while processing: %s', result)
        return 0.


def process_event(event, namespace, prometheus_url):
    pod = event['object']
    username = pod.metadata.annotations.get(USERNAME_ANNOTATION,
                                            DEFAULT_USERNAME)
    logging.debug("Got %s for pod %s (user: %s)",
                  event['type'],
                  pod.metadata.uid,
                  username)

    try:
        notebook = Notebook.get(Notebook.uid == pod.metadata.uid)
    except Notebook.DoesNotExist:
        notebook = Notebook(uid=pod.metadata.uid, username=username)

    if not notebook.start:
        # first time we see the pod, use current time as start
        notebook.start = datetime.datetime.now()

    if event['type'] == 'DELETED' and not notebook.end:
        # make sure we capture some end time
        notebook.end = datetime.datetime.now()

    if pod.status.container_statuses:
        state = pod.status.container_statuses[0].state
        logging.debug("Pod state: %s" % state)
        if state.running and state.running.started_at:
            logging.debug("Got start date from k8s: %s", state.running.started_at)
            notebook.start = state.running.started_at
        if state.terminated and state.terminated.finished_at:
            logging.debug("Got terminated date from k8s: %s", state.terminated.finished_at)
            notebook.end = state.terminated.finished_at

    if notebook.end:
        notebook.cpu_time = get_usage_info(prometheus_url,
                                           namespace,
                                           pod.metadata.name)
    notebook.save()


def watch(namespace='', prometheus_url=''):
    kubernetes.config.load_incluster_config()
    v1 = kubernetes.client.CoreV1Api()
    w = kubernetes.watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, namespace=namespace,
                          label_selector='component=singleuser-server'):
        process_event(event, namespace, prometheus_url)


def main():
    logging.basicConfig(level=logging.DEBUG)
    namespace = os.environ.get(NAMESPACE_ENV, DEFAULT_NAMESPACE)
    logging.debug('Namespace to watch: %s', namespace)
    prometheus_url = os.environ.get(PROMETHEUS_URL_ENV, DEFAULT_PROMETHEUS_URL)
    logging.debug('Prometheus server at %s', prometheus_url)
    db_file = os.environ.get(NOTEBOOK_DB_ENV, DEFAULT_NOTEBOOK_DB)
    logging.debug('Creating DB at %s', db_file)
    db.init(db_file)
    db.connect()
    db.create_tables([Notebook])
    watch(namespace, prometheus_url)


if __name__ == '__main__':
    main()
