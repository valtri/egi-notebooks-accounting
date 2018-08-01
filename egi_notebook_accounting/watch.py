import datetime
import json
import os

from kubernetes import client, config, watch
import requests


def main(namespace='', prometheus_url='http://localhost:9000', spool_dir='spool'):
    config.load_incluster_config()

    v1 = client.CoreV1Api()

    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, namespace=namespace, label_selector='component=singleuser-server'):
        pod = event['object']
        print("Event: %s %s %s" % (event['type'],event['object'].kind, event['object'].metadata.name))
        #print("E2: %s %s %s" % (event['object'].metadata.uid,event['object'].kind, event['object'].metadata.name))
        #print("Event: %s %s" % (event, dir(event)))
        print("POD: %s" % (pod.metadata.uid))
        print("ANN: %s" % (pod.metadata.annotations))

        pod_file = os.path.join(spool_dir, pod.metadata.uid)
        notebook = {'uid': pod.metadata.uid,
                    'user': pod.metadata.annotations.get('hub.jupyter.org/username')}
        try:
            if os.path.exists(pod_file):
                with open(pod_file, 'r') as f:
                    notebook = json.loads(f.read())
        except Exception as e:
            print(e)
            print(pod_file)

        if pod.status.container_statuses:
            state = pod.status.container_statuses[0].state
            print("STATE: %s" % pod.status.container_statuses)
            if state.running:
                print("RUN: %s" % pod.status.container_statuses[0].state.running.started_at)
                if state.running.started_at:
                    notebook['start'] = state.running.started_at.isoformat()
                else:
                    notebook['start'] = datetime.datetime.now().isoformat()
            if state.terminated:
                # Gone, if we dont have a terminated at, we craft one
                print("DEL: %s" % pod.status.container_statuses[0].state.terminated.finished_at)
                if state.terminated.finished_at:
                    notebook['end'] = state.terminated.finished_at.isoformat()
                else:
                    notebook['end'] = datetime.datetime.now().isoformat()
                # Making multiple queries here
                query = ("container_cpu_usage_seconds_total{{namespace='{0}',"
                         "pod_name='{1}',container_name='notebook'}}").format(namespace, pod.metadata.name)
                r = requests.get('{0}/api/v1/query'.format(prometheus_url),
                                     params={'query': query})
                try:
                    print("HERE!")
                    print(r.json()['data']['result'])
                    print(r.json()['data']['result'][-1]['value'][1])
                    notebook['cpu_time'] = r.json()['data']['result'][-1]['value'][1]
                except:
                    pass
        print("NOTEBOOK: %s" % notebook)
        with open(pod_file, 'w+') as f:
            f.write(json.dumps(notebook))


if __name__ == '__main__':
    main('training', 'http://prom-prometheus-server.default.svc.cluster.local')
