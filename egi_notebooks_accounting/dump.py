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


@attr.s
class VMRecord:
    local_id = attr.ib(default='')
    site = attr.ib(default=None)
    machine = attr.ib(default=None)
    local_user_id = attr.ib(default=None)
    local_group_id = attr.ib(default=None)
    global_user_name = attr.ib(default=None)
    fqan = attr.ib(default=None)
    status = attr.ib(default=None)
    start_time = attr.ib(default=None)
    end_time = attr.ib(default=None)
    suspend_duration = attr.ib(default=0)
    wall = attr.ib(default=0)
    cpu = attr.ib(default=0)
    cpu_count = attr.ib(default=1)
    network_type = attr.ib(default=None)
    network_inbound = attr.ib(default=0)
    network_outbound = attr.ib(default=0)
    memory = attr.ib(default=0)
    disk = attr.ib(default=0)
    storage_record = attr.ib(default=None)
    image_id = attr.ib(default=None)
    cloud_type = attr.ib(default=None)
    self.cloud_compute_service = attr.ib(default=None)
    self.benchmark_type = attr.ib(default=None)
    self.benchmark = attr.ib(default=None)
    self.public_ip_count = attr.ib(default=0)

    def as_dict(self):
        return {
            'VMUUID': self.local_id,
            'SiteName': self.site,
            'MachineName': self.machine,
            'LocalUserId': self.local_user_id,
            'LocalGroupId': self.,
            'GlobalUserName': self.global_user_name,
            'FQAN': self.fqan,
            'Status': self.,
            'StartTime': self.start_time,
            'EndTime': self.end_time,
            'SuspendDuration': self.suspend_duration,
            'WallDuration': self.,
            'CpuDuration': self.cpu,
            'CpuCount': self.cpu_count,
            'NetworkType': self.network_type,
            'NetworkInbound': self.network_inbound,
            'NetworkOutbound': self.network_outbound,
            'Memory': self.memory,
            'Disk': self.disk,
            'StorageRecordId': self.storage_record,
            'ImageId': self.image_id,
            'CloudType': self.cloud_type,
            'CloudComputeService': self.cloud_compute_service,
            'BenchmarkType': self.benchmark_type,
            'Benchmark': self.benchmark,
            'PublicIPCount': self.public_ip_count,
        }

    def dump(self):
        record = []
        for k, v in self.as_dict().items():
            if v is not None:
                record.append('{0}: {1}'.format(k, v))
        return '\n'.join(record)

    @classmethod
    def from_notebook(cls, notebook, **defaults):
        record = cls(global_user_name=notebook.username,
                     local_id=notebook.uid,
                     cpu=notebook.cpu_time,
                     start_time=notebook.start,
                     **defaults)
        if notebook.end and notebook.start:
            record.end = notebook.end
            record.wall = notebook.end - notebook.start


def get_usage_stats(prometheus_url, namespace):
    logging.debug("Getting usage information from prometheus")
    end = int(time.time())
    # Go back 6 hours
    # TODO(enolfc): what's the right interval to use?
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


def dump(prometheus_url, namespace, spool_dir, site_config={}):
    db.connect()
    pod_usage = get_usage_stats(prometheus_url, namespace)
    records = []
    processed_notebooks = []
    for notebook in Notebook.select().where(Notebook.processed == False):
        notebook.cpu_time = pod_usage.get(notebook.uid, 0.0)
        records.append(JobRecord.from_notebook(notebook).dump(), **site_config)
        if notebook.end:
            processed_notebooks.append(notebook)
    message = '\n'.join(['APEL-individual-job-message: v0.3',
                         '\n%%\n'.join(records)])
    queue = QueueSimple.QueueSimple(spool_dir)
    queue.add(message)
    logging.debug("Dumped %d records to spool dir", len(records))
    # once dumped, set the notebooks as processed if finished
    for notebook in processed_notebooks:
        notebook.processed = True
        notebook.save()
    db.close()


def main():
    logging.basicConfig(level=logging.DEBUG)
    init_db()
    namespace = os.environ.get('NAMESPACE', DEFAULT_NAMESPACE)
    logging.debug('Watched namespace: %s', namespace)
    prometheus_url = os.environ.get('PROMETHEUS_URL', DEFAULT_PROMETHEUS_URL)
    logging.debug('Prometheus server at %s', prometheus_url)
    site_config = dict(site=os.environ.get('SITENAME', ''),
                       machine=os.environ.get('MACHINE', ''),
                       fqan=os.environ.get('VO', 'access.egi.eu'),
                       cloud_type=os.environ.get('CLOUD_TYPE',
                                                 'EGI Notebooks'),
                       cloud_compute_service=os.environ('SERVICE', ''))
    dump(prometheus_url, namespace, '', site_config)
    # and every 6 hours from now
    # this could be just a Kubernetes cron job no?
    schedule.every(6).minutes.do(dump, prometheus_url, namespace, '',
                                 site_config)
    while True:
        schedule.run_pending()
        # 5 minutes sleep, am I using the right tool here?
        time.sleep(300)


if __name__ == '__main__':
    main()
