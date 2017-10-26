"""
Mock Analytics/Execution Engine Class for testing

"""

from __future__ import absolute_import, print_function

from threading import Thread
from celery import Celery

from .decomposer import AnalyticsEngineV2
from datacube.execution.execution_engine2 import ExecutionEngineV2
from datacube.config import LocalConfig


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
    app.control.shutdown(timeout=20)

@app.task
def run_python_function_base(function, data, storage_params=None, config=None, *args, **kwargs):
    '''Process the function and data submitted by the user.'''
    store_config = config['redis']
    driver_manager_config = config['datacube']
    analytics_engine = AnalyticsEngineV2(store_config, driver_manager_config, function, data, storage_params)
    jobs, jro, base_results = analytics_engine.analyse()
    results = []
    for job in jobs:
        results.append(run_python_function_subjob.delay(job, base_results, config, *args, **kwargs))

    # TODO: Fix returning the jro
    jro = 'JRO'
    return (jro, results)

@app.task
def run_python_function_subjob(job, base_results, config=None, *args, **kwargs):
    '''Process a subjob, created by the base job.'''
    store_config = config['redis']
    driver_manager_config = config['datacube']
    execution_engine = ExecutionEngineV2(store_config, driver_manager_config, job)
    return execution_engine.execute(base_results)
