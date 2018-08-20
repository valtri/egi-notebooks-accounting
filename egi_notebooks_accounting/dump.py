from datetime import datetime
import logging
import os
import pprint
import time

import attr
from dirq import QueueSimple
import requests
import schedule

from .model import db, init_db, Notebook

DEFAULT_NAMESPACE = 'default'
DEFAULT_PROMETHEUS_URL = 'http://localhost:9000'
# name of the environment variables where config is expected
NAMESPACE_ENV = 'NOTEBOOKS_NS'
PROMETHEUS_URL_ENV = 'PROMETHEUS_URL'


@attr.s
class JobRecord:
    site = attr.ib(default=None)
    submit_host = attr.ib(default=None)
    machine_name = attr.ib(default=None)
    local_job_id = attr.ib(default=None)
    global_user_name = attr.ib(default=None)
    wall = attr.ib(default=0)
    cpu = attr.ib(default=0)
    start_time = attr.ib(default=None)
    end_time = attr.ib(default=None)
    fqan = attr.ib(default=None)
    infrastructure_description = attr.ib(default='')
    infrastructure_type = attr.ib(default='')
    mem_real = attr.ib(default=0)
    mem_virtual = attr.ib(default=0)
    local_user_id = attr.ib(default='')
    processors = attr.ib(default=1)
    node_count = attr.ib(default=1)
    # use here the namespace?
    queue = attr.ib(default='k8s')
    scaling_factor_unit = attr.ib(default='custom')
    scaling_factor = attr.ib(default=1)

    def as_dict(self):
        return {
            'Site': self.site,
            'SubmitHost': self.submit_host,
            'MachineName': self.machine_name,
            'Queue': self.queue,
            'LocalJobId': self.local_job_id,
            'LocalUserId': self.local_user_id,
            'GlobalUserName': self.global_user_name,
            'UserFQAN': self.fqan,
            'WallDuration': self.wall,
            'CpuDuration': self.cpu,
            'Processors': self.processors,
            'NodeCount': self.node_count,
            'StartTime': self.start_time,
            'EndTime': self.end_time,
            'InfrastructureDescription': self.infrastructure_description,
            'InfrastructureType': self.infrastructure_type,
            'MemoryReal': self.mem_real,
            'MemoryVirtual': self.mem_virtual,
            'ScalingFactorUnit': self.scaling_factor_unit,
            'ScalingFactor': self.scaling_factor,
        }

    def dump(self):
        record = []
        for k, v in self.as_dict().items():
            if v is not None:
                record.append('{0}: {1}'.format(k, v))
        return '\n'.join(record)

    @classmethod
    def from_notebook(cls, notebook, **defaults):
        return cls(global_user_name=notebook.username,
                   local_job_id=notebook.uid,
                   cpu=notebook.cpu_time,
                   start_time=notebook.start,
                   end_time=notebook.end,
                   wall=(notebook.end - notebook.start))


def get_usage_stats(prometheus_url, namespace):
    logging.debug("Getting usage information from prometheus")
    end = int(time.time())
    # 6 hours before now
    start = end - 6 * 3600
    query_str = ("container_cpu_usage_seconds_total{{namespace='{0}',"
                 "pod_name=~'^jupyter-.*',container_name='notebook'}}")
    params = {'query': query_str.format(namespace),
              # is this 4h safe enough?
              'start': start, 'end': end, 'step': '4h'}
    r = requests.get('{0}/api/v1/query_range'.format(prometheus_url), params=params)
    pods = {}
    for result in r.json()['data']['result']:
        # assuming this will be always in the same format
        # not very reliable if you ask me ;)
        name = result['metric']['name'].split('_')[-2]
        # last known value
        pods[name] = result['values'][-1][1]
    return pods


def dump(prometheus_url, namespace, spool_dir):
    db.connect()
    pod_usage = get_usage_stats(prometheus_url, namespace)
    records = []
    for notebook in Notebook.select().where(Notebook.processed == False,
                                            Notebook.end != None):
        notebook.cpu_time = pod_usage.get(notebook.uid, 0.0)
        records.append(JobRecord.from_notebook(notebook).dump())
    message = '\n'.join(['APEL-individual-job-message: v0.3',
                         '\n%%\n'.join(records)])
    queue = QueueSimple.QueueSimple(spool_dir)
    queue.add(message)
    logging.debug("Dumped %d records to spool dir", len(records))
    # once dumped, set the notebooks as processed
    for notebook in Notebook.select().where(Notebook.processed == False,
                                            Notebook.end != None):
        notebook.processed = True
        notebook.save()
    db.close()


def main():
    logging.basicConfig(level=logging.DEBUG)
    init_db()
    namespace = os.environ.get(NAMESPACE_ENV, DEFAULT_NAMESPACE)
    logging.debug('Namespace to watch: %s', namespace)
    prometheus_url = os.environ.get(PROMETHEUS_URL_ENV, DEFAULT_PROMETHEUS_URL)
    logging.debug('Prometheus server at %s', prometheus_url)
    # first dump now
    dump(prometheus_url, namespace, '')
    # and every 6 hours from now
    schedule.every(6).minutes.do(dump, prometheus_url, namespace, '')
    while True:
        schedule.run_pending()
        # 5 minutes sleep, am I using the right tool here?
        time.sleep(300)

if __name__ == '__main__':
    main()
