import datetime
import logging

import kubernetes

from .model import db, init_db, Notebook
from .utils import get_k8s_namespace


def process_event(event, namespace):
    pod = event['object']
    username = pod.metadata.annotations.get('hub.jupyter.org/username',
                                            'nobody')
    logging.debug("Got %s for pod %s (user: %s)",
                  event['type'],
                  pod.metadata.uid,
                  username)

    db.connect()
    try:
        notebook = Notebook.get(Notebook.uid == pod.metadata.uid)
    except Notebook.DoesNotExist:
        notebook = Notebook(uid=pod.metadata.uid, username=username)

    # make sure we capture some end time
    if event['type'] == 'DELETED' and not notebook.end:
        notebook.end = datetime.datetime.now().timestamp()

    if pod.status.container_statuses:
        state = pod.status.container_statuses[0].state
        logging.debug("Pod state: %s" % state)
        if state.running and state.running.started_at:
            logging.debug("Got start date from k8s: %s",
                          state.running.started_at)
            notebook.start = state.running.started_at.timestamp()
        if state.terminated and state.terminated.finished_at:
            logging.debug("Got terminated date from k8s: %s",
                          state.terminated.finished_at)
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
    namespace = get_k8s_namespace()
    logging.debug('Watching namespace: %s', namespace)
    watch(namespace)


if __name__ == '__main__':
    main()
