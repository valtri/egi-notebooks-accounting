import datetime
import logging
import json
import os

import kubernetes
import peewee
import requests

from .model import db, init_db, Notebook

# Some constants, may be moved somewhere else
USERNAME_ANNOTATION = 'hub.jupyter.org/username'
DEFAULT_USERNAME = 'nobody'
DEFAULT_NAMESPACE = 'default'
DEFAULT_PROMETHEUS_URL = 'http://localhost:9000'
# name of the environment variables where config is expected
NAMESPACE_ENV = 'NOTEBOOKS_NS'
PROMETHEUS_URL_ENV = 'PROMETHEUS_URL'


def process_event(event, namespace):
    pod = event['object']
    username = pod.metadata.annotations.get(USERNAME_ANNOTATION,
                                            DEFAULT_USERNAME)
    logging.debug("Got %s for pod %s (user: %s)",
                  event['type'],
                  pod.metadata.uid,
                  username)

    db.connect()
    try:
        notebook = Notebook.get(Notebook.uid == pod.metadata.uid)
    except Notebook.DoesNotExist:
        notebook = Notebook(uid=pod.metadata.uid, username=username)

    if event['type'] == 'DELETED' and not notebook.end:
        # make sure we capture some end time
        notebook.end = datetime.datetime.now().timestamp()

    if pod.status.container_statuses:
        state = pod.status.container_statuses[0].state
        logging.debug("Pod state: %s" % state)
        if state.running and state.running.started_at:
            logging.debug("Got start date from k8s: %s", state.running.started_at)
            notebook.start = state.running.started_at.timestamp()
        if state.terminated and state.terminated.finished_at:
            logging.debug("Got terminated date from k8s: %s", state.terminated.finished_at)
            notebook.end = state.terminated.finished_at.timestamp()

    notebook.save()
    db.close()


def watch(namespace=''):
    kubernetes.config.load_incluster_config()
    v1 = kubernetes.client.CoreV1Api()
    w = kubernetes.watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, namespace=namespace,
                          label_selector='component=singleuser-server'):
        process_event(event, namespace) 


def main():
    logging.basicConfig(level=logging.DEBUG)
    init_db()
    namespace = os.environ.get(NAMESPACE_ENV, DEFAULT_NAMESPACE)
    logging.debug('Watching namespace: %s', namespace)
    watch(namespace)


if __name__ == '__main__':
    main()
