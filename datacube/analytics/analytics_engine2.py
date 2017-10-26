"""
Mock Analytics/Execution Engine Class for testing

"""

from __future__ import absolute_import, print_function

#import logging
#import sys
from threading import Thread
#from uuid import uuid4
#from pprint import pformat
from celery import Celery

#from datacube import Datacube
from .decomposer import AnalyticsEngineV2
from datacube.drivers.manager import DriverManager
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


def launch_ae_worker(store_config):
    if store_config is None:
        local_config = LocalConfig.find()
        store_config = local_config.redis_celery_config

    if 'password' in store_config:
        url = 'redis://{}:{}/{}'.format(store_config['host'], store_config['port'], store_config['db'])
    else:
        url = 'redis://:{}@{}:{}/{}'.format(store_config['password'], store_config['host'],
                                            store_config['port'], store_config['db'])

    Thread(target=launch_worker_thread, args=(url,)).start()


def launch_worker_thread(url):
    app.conf.update(result_backend=url,
                    broker_url=url)

    argv = ['worker', '-A', 'datacube.analytics.analytics_engine2', '-l', 'DEBUG']

    app.worker_main(argv)


def stop_worker():
    app.control.shutdown()


@app.task
def run_python_function_base(function, data, storage_params=None, config=None, *args, **kwargs):
    # submit_python_function & fake_base_worker_thread code goes here
    # change function parameters accordingly
    '''
    store = config['redis']
    driver_manager = DriverManager(local_config=config['datacube'])
    analytics_engine = AnalyticsEngineV2(store, driver_manager)
    jobs, jro = analytics_engine.analyse(function, data, storage_params)
    # embedding celery async results for testing JRO state tracking.
    results = []
    for job in jobs:
        results.append(run_python_function_subjob.delay(job, storage_params, config, *args, **kwargs))
    return (jro, results)
    '''

    # Example code below
    results = []
    # submit a new job per job in decomposed['jobs']
    results.append(run_python_function_subjob.delay(function, data, storage_params, config, *args, **kwargs))
    return results


@app.task
def run_python_function_subjob(function, data, storage_params=None, config=None, *args, **kwargs):
    # fake_worker_thread code goes here.
    # change function parameters accordingly

    store = config['redis']
    driver_manager = DriverManager(local_config=config['datacube'])
    execution_engine = ExecutionEngineV2(store, driver_manager, {'id': 0})
    # execution_engine.execute()

    # Example code below
    # analytics_engine = AnalyticsEngineV2(store, driver_manager)
    # nbar = analytics_engine.dc.load(use_threads=True, **data)

    # pylint: disable=protected-access
    nbar = execution_engine._get_data(data)
    from pprint import pprint
    pprint(nbar)

    print("Python function executed")
    return nbar
