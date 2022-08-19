#! /usr/bin/python3

#
# Script to generate accounting to local database from the old APEL dumps
#
# Merge tables after creating the records:
#
# ALTER TABLE "vm" RENAME TO "vm_orig";
# CREATE TABLE "vm" AS SELECT * FROM vm_orig UNION SELECT * FROM vm_old WHERE local_id NOT IN (SELECT local_id FROM vm_orig);
# DROP TABLE vm_orig;
#

import argparse
import logging
import os
import peewee
import re
import stat
from configparser import ConfigParser
from datetime import datetime

from .model import VM, db, db_init


CONFIG = 'prometheus'
DEFAULT_CONFIG_FILE = 'config.ini'
db_file = None


class VMOld(VM):
    def __init__(self):
        super().__init__()


def from_dict(r):
    pod = VMOld()
    for k in r.keys():
        v = r[k]
        if k == 'VMUUID':
            pod.local_id = v
        elif k == 'SiteName':
            pod.site = v
        elif k == 'MachineName':
            pod.machine = v
        elif k == 'LocalUserId':
            pod.local_user_id = v
        elif k == 'LocalGroupId':
            pod.local_group_id = v
        elif k == 'GlobalUserName':
            pod.global_user_name = v
        elif k == 'FQAN':
            pod.fqan = v
        elif k == 'Status':
            pod.status = v
        elif k == 'StartTime':
            pod.start_time = datetime.fromtimestamp(int(v))
        elif k == 'EndTime':
            pod.end_time = datetime.fromtimestamp(int(v))
        elif k == 'SuspendDuration':
            pod.suspend_duration = v
        elif k == 'WallDuration':
            pod.wall = v
        elif k == 'CpuDuration':
            pod.cpu_duration = v
        elif k == 'CpuCount':
            pod.cpu_count = v
        elif k == 'NetworkType':
            pod.network_type = v
        elif k == 'NetworkInbound':
            pod.network_inbound = v
        elif k == 'NetworkOutbound':
            pod.network_outbound = v
        elif k == 'Memory':
            pod.memory = v
        elif k == 'Disk':
            pod.disk = v
        elif k == 'StorageRecordId':
            pod.storage_record = v
        elif k == 'ImageId':
            pod.image_id = v
        elif k == 'CloudType':
            pod.cloud_type = v
        elif k == 'CloudComputeService':
            pod.cloud_compute_service = v
        elif k == 'BenchmarkType':
            pod.benchmark_type = v
        elif k == 'Benchmark':
            pod.benchmark = v
        elif k == 'PublicIPCount':
            pod.public_ip_count = v

    if pod.namespace is None:
        if pod.global_user_name is None:
            pod.namespace = 'unknown'
        elif '@egi.eu' in pod.global_user_name:
            pod.namespace = 'hub'
        else:
            pod.namespace = 'binder'

    return pod


def save_pod(r):
    global db_file

    pod = from_dict(r)
    print('POD: %s\n' % pod.dump())

    if db_file is None:
        return
    try:
        pod.save(force_insert=True)
    except peewee.IntegrityError:
        pod.save()


def import_dump(f):
    line = f.readline()
    while line and not line.startswith('APEL-cloud-message:'):
        line = f.readline()
    pod = None
    while line:
        if not line or line.rstrip() == '%%':
            if pod is not None:
                save_pod(pod)
                pod = None
            if line:
                line = f.readline()
        else:
            if pod is None:
                pod = dict()
            a = re.split(r': ', line.rstrip(), maxsplit=2)
            if len(a) == 2:
                pod[a[0]] = a[1]
            line = f.readline()
    if pod is not None:
        save_pod(pod)


def main():
    global db_file

    parser = argparse.ArgumentParser(description='Importer from APEL dump')
    parser.add_argument('-c', '--config', help='config file', default=DEFAULT_CONFIG_FILE)
    args = parser.parse_args()

    parser = ConfigParser()
    parser.read(args.config)
    config = parser[CONFIG] if CONFIG in parser else {}

    logging.basicConfig(level=logging.DEBUG)

    db_file = os.environ.get('NOTEBOOKS_DB', config.get('notebooks_db', None))
    if db_file:
        db_init(db_file)
        db.connect()
        VMOld._meta.set_table_name('vm_old')
        db.create_tables([VMOld])

    stats = dict()
    for root, dirs, files in os.walk('apel'):
        for name in files:
            spec = os.path.join(root, name)
            statinfo = os.stat(spec)
            if stat.S_ISREG(statinfo.st_mode):
                stats[spec] = statinfo.st_mtime
    for spec, t in sorted(stats.items(), key=lambda x: x[1]):
        print('APEL DUMP: %s' % spec)
        with open(spec, 'r') as f:
            import_dump(f)

    if db_file:
        db.close()


if __name__ == '__main__':
    main()
