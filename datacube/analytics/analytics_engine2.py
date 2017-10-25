"""
Mock Analytics/Execution Engine Class for testing

"""

from __future__ import absolute_import, print_function

#import logging
#import sys
#from threading import Thread
#from uuid import uuid4
#from pprint import pformat
from celery import Celery

#from datacube import Datacube
from .decomposer import AnalyticsEngineV2
from datacube.execution.execution_engine2 import ExecutionEngineV2
#from .utils.store_handler import FunctionTypes, JobStatuses, ResultTypes, ResultMetadata, StoreHandler
from datacube.config import LocalConfig
#from datacube.analytics.job_result import JobResult, LoadType
#from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO
#from datacube.drivers.s3.storage.s3aio.s3io import S3IO

def celery_app(store_config=None):

    if store_config is None:
        local_config = LocalConfig.find()
        store_config = local_config.redis_celery_config

    if 'password' in store_config:
        url = 'redis://{}:{}/{}'.format(store_config['host'], store_config['port'], store_config['db'])
    else:
        url = 'redis://:{}@{}:{}/{}'.format(store_config['password'], store_config['host'],
                                            store_config['port'], store_config['db'])

    _app = Celery('ee_task', broker=url, backend=url)

    _app.conf.update(
        task_serializer='pickle',
        result_serializer='pickle',
        accept_content=['pickle'])

    return _app


def engine(store_config=None, driver_manager=None):
    if store_config is None:
        local_config = LocalConfig.find()
        store_config = local_config.redis_config
    print(store_config)

    return AnalyticsEngineV2(store_config, driver_manager=driver_manager)


# pylint: disable=invalid-name
app = celery_app()


@app.task
def run_python_function_base(function, data, storage_params=None, *args, **kwargs):
    # submit_python_function & fake_base_worker_thread code goes here
    # change function parameters accordingly

    store = None  # TODO: create from config passed through celery
    driver_manager = None  # TODO: create from config passed through celery
    analytics_engine = AnalyticsEngineV2(store, driver_manager)
    jobs, jro = analytics_engine.analyse()
    for job in jobs:
        run_python_function_subjob.delay(job, storage_params, *args, **kwargs)
    return jro


@app.task
def run_python_function_subjob(function, data, storage_params=None, *args, **kwargs):
    # fake_worker_thread code goes here.
    # change function parameters accordingly

    store = None  # TODO: create from config passed through celery
    driver_manager = None  # TODO: create from config passed through celery
    execution_engine = ExecutionEngineV2(store, driver_manager)
    execution_engine.execute()


    # example below to demonstrate sub job executes.
    from pprint import pprint
    data = {
        'product': 'ls5_nbar_albers',
        'measurements': ['blue', 'red'],
        'x': (149.25, 149.35),
        'y': (-35.25, -35.35)
    }
    ae = engine()
    nbar = ae.dc.load(use_threads=True, **data)
    pprint(nbar)

    print("Python function executed")
    return "Python function executed"
