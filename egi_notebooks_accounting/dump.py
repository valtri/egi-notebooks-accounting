from datetime import datetime
import logging
import os
import pprint
import time

import attr
import schedule

from .model import db, init_db, Notebook

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
        d = {k: v for (k, v) in self.as_dict().items() if v is not None}
        return pprint.pformat(d)

    @classmethod
    def from_notebook(cls, notebook, **defaults):
        return cls(global_user_name=notebook.username,
                   local_job_id=notebook.uid,
                   cpu=notebook.cpu_time,
                   start_time=notebook.start,
                   end_time=notebook.end,
                   wall=(notebook.end - notebook.start))


def dump(spool_dir):
    db.connect()
    print("*" * 80)
    for notebook in Notebook.select().where(Notebook.processed == False,
                                            Notebook.end != None):
        print(JobRecord.from_notebook(notebook).dump())
        notebook.processed = True
    print("*" * 80)
    db.close()


def main():
    logging.basicConfig(level=logging.DEBUG)
    init_db()
    dump('')
    schedule.every(1).minute.do(dump, '')
    while True:
        schedule.run_pending()
        time.sleep(10)

if __name__ == '__main__':
    main()
