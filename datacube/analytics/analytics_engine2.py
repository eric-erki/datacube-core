'''Analytics engine entry point.'''

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
    _app = Celery('ee_task', broker=store_config['url'], backend=store_config['url'])
    _app.conf.update(
        task_serializer='pickle',
        result_serializer='pickle',
        accept_content=['pickle'])
    return _app

#def engine(config=None, driver_manager=None):
#    if config is None:
#        config = LocalConfig.find()
#    store_config = config.redis_config
#    print(store_config)
#    return AnalyticsEngineV2(store_config, driver_manager=driver_manager)

# pylint: disable=invalid-name
app = celery_app()

def launch_ae_worker(store_config):
    if store_config is None:
        local_config = LocalConfig.find()
        store_config = local_config.redis_celery_config
    Thread(target=launch_worker_thread, args=(store_config['url'],)).start()

def launch_worker_thread(url):
    app.conf.update(result_backend=url,
                    broker_url=url)
    argv = ['worker', '-A', 'datacube.analytics.analytics_engine2', '-l', 'DEBUG']
    app.worker_main(argv)

def stop_worker():
    app.control.shutdown()

@app.task
def run_python_function_base(config, function, data, storage_params=None, *args, **kwargs):
    '''Process the function and data submitted by the user.'''
    analytics_engine = AnalyticsEngineV2(config, function, data, storage_params)
    jobs, jro, base_results = analytics_engine.analyse(*args, **kwargs)
    results = []
    for job in jobs:
        results.append(run_python_function_subjob.delay(config, job, base_results, *args, **kwargs))
    return (jro, results)

@app.task
def run_python_function_subjob(config, job, base_results, *args, **kwargs):
    '''Process a subjob, created by the base job.'''
    execution_engine = ExecutionEngineV2(config, job)
    return execution_engine.execute(base_results, *args, **kwargs)
