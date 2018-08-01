import datetime
import logging
import json
import os

from kubernetes import client, config, watch
import requests


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
    username = pod.metadata.annotations.get('hub.jupyter.org/username',
                                            'nobody')
    logging.debug("Got %s for pod %s (user: %s)",
                  event['type'],
                  pod.medata.uid,
                  username)

    pod_file = os.path.join(spool_dir, pod.metadata.uid)
    notebook = {'uid': pod.metadata.uid,
                'user': username}
    try:
        if os.path.exists(pod_file):
            with open(pod_file, 'r') as f:
                notebook = json.loads(f.read())
    except:
        # do not care much, may be the first time we see the pod
        pass

    if pod.status.container_statuses:
        state = pod.status.container_statuses[0].state
        if state.running:
            log.debug("Pod is running: %s", state.running)
            if state.running.started_at:
                notebook['start'] = state.running.started_at.isoformat()
            else:
                notebook['start'] = datetime.datetime.now().isoformat()
        if state.terminated:
            log.debug("Pod is terminated: %s", state.terminated)
            if state.terminated.finished_at:
                notebook['end'] = state.terminated.finished_at.isoformat()
            else:
                notebook['end'] = datetime.datetime.now().isoformat()
            # Making multiple queries here
            notebook['cpu_time'] = get_usage_info(prometheus_url,
                                                  namespace,
                                                  pod.metadata.name)
    log.debug("Write pod_file %s with %s", pod_file, notebook)
    with open(pod_file, 'w+') as f:
        f.write(json.dumps(notebook))


def watch(namespace='', prometheus_url='', spool_dir='spool'):
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, namespace=namespace,
                          label_selector='component=singleuser-server'):
        process_event(event, namespace, prometheus_url, spool_dir)


def main():
    watch('training', 'http://prom-prometheus-server.default.svc.cluster.local')


if __name__ == '__main__':
    main()
