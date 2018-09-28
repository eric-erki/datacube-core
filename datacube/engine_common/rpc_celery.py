from __future__ import absolute_import

from time import sleep
from celery import Celery

from datacube.config import LocalConfig
from datacube.analytics.analytics_worker import AnalyticsWorker

# pylint: disable=invalid-name
config = None


# TODO: In production environment, the engines need to be started using a local config identified
# through `find()`. This is not desirable in pytest as it will use the default config which is
# invalid and crashes all the tests. For now, we simply check whether this is run within
# pytest. This must be addressed another way.
# if 'pytest' not in modules:
# initialise_engines()


def celery_app(store_config=None):
    try:
        if store_config is None:
            local_config = LocalConfig.find()
            store_config = local_config.redis_celery_config
        _app = Celery('ee_task', broker=store_config['url'], backend=store_config['url'])
    except ValueError:
        _app = Celery('ee_task')

    _app.conf.update(
        task_serializer='pickle',
        result_serializer='pickle',
        accept_content=['pickle'],
        worker_prefetch_multiplier=1,
        broker_pool_limit=100,
        broker_connection_retry=True,
        broker_connection_timeout=4,
        broker_transport_options={'socket_keepalive': True, 'retry_on_timeout': True,
                                  'socket_connect_timeout': 10, 'socket_timeout': 10},
        redis_socket_connect_timeout=10,
        redis_socket_timeout=10)
    return _app

# pylint: disable=invalid-name
app = celery_app()


def launch_ae_worker(local_config):
    """Only used for pytests"""
    if not local_config:
        local_config = LocalConfig.find()
    # pylint: disable=global-statement
    global config
    config = local_config
    store_config = local_config.redis_celery_config
    # initialise_engines(local_config)
    from multiprocessing import Process
    process = Process(target=launch_worker_thread, args=(store_config['url'],))
    process.start()
    return process


def launch_worker_thread(url):
    """Only used for pytests"""
    app.conf.update(result_backend=url,
                    broker_url=url)
    argv = ['worker', '-A', 'datacube.engine_common.rpc_celery', '-l', 'INFO', '--autoscale=3,0']
    app.worker_main(argv)


def stop_worker():
    """Only used for pytests"""
    app.control.shutdown()



class RPCCelery():
    '''The celery remote procedure call is to be used as plug in to call
remote functions during for the analytics engine.'''

    def __init__(self, dc_config):
        '''Update the celery app config.'''
        # global app
        app.conf.update(result_backend=dc_config.redis_celery_config['url'],
                        broker_url=dc_config.redis_celery_config['url'])

    def _run_python_function_base(self, url):
        '''Run the function using celery.

        This is placed in a separate method so it can be overridden
        during tests.
        '''
        analysis_p = app.send_task('datacube.engine_common.rpc_celery.run_python_function_base',
                                   args=(url,))
        last_error = None
        for attempt in range(50):
            # pylint: disable=broad-except
            try:
                while not analysis_p.ready():
                    sleep(1.0)
                return analysis_p.get(disable_sync_subtasks=False)
            except ValueError as e:
                raise e
            except TimeoutError as e:
                last_error = str(e)
                print("error - AnalyticsClient._run_python_function_base()", str(type(e)), last_error)
                sleep(0.5)
                continue
            except Exception as e:
                last_error = str(e)
                print("error u - AnalyticsClient._run_python_function_base()", str(type(e)), last_error)
                sleep(0.5)
                continue

    def _get_update(self, action, item_id, paths=None, env=None, max_retries=50):
        '''Remotely invoke the `analytics_worker.get_update()` method.'''
        # Minimal check: item ID must be an int
        if not isinstance(item_id, int):
            raise ValueError('Invalid job or result id: {}'.format(item_id))
        data_p = app.send_task('datacube.engine_common.rpc_celery.get_update',
                               args=(action, item_id, paths, env))
        last_error = None
        for attempt in range(max_retries):
            # pylint: disable=broad-except
            try:
                while not data_p.ready():
                    sleep(1.0)
                return data_p.get(disable_sync_subtasks=False)
                # return data_p.get(disable_sync_subtasks=False)
            except TimeoutError as e:
                last_error = str(e)
                print("error - AnalyticsClient._get_update()", str(type(e)), last_error)
                sleep(0.5)
                continue
            except Exception as e:
                last_error = str(e)
                print("error u - AnalyticsClient._get_update()", str(type(e)), last_error)
                sleep(0.5)
                continue

        # Exceeded max retries
        raise RuntimeError('AnalyticsClient._get_update', 'exceeded max retries', last_error)

    def __repr__(self):
        '''Return some information about the currently active workers.'''
        active = app.control.inspect().active()
        workers = [task['name'].split('.')[-1] for task in active.popitem()[1]] if active else []
        return '{} active workers: {}'.format(len(workers), workers)


@app.task
def run_python_function_subjob(url):
    '''Process a subjob, created by the base job.

    This celery task calls the original implementation in the parent
    class.
    '''
    return celery_analytics_worker.original_run_python_function_subjob(url)

@app.task
def monitor_jobs(decomposed, subjob_tasks, params_url):
    '''Monitors base job.

    This celery task calls the original implementation in the parent
    class.
    '''
    return celery_analytics_worker.original_monitor_jobs(decomposed, subjob_tasks, params_url)

@app.task
def get_update(action, item_id, paths=None, env=None):
    '''Return an update on a job or result.'''
    return celery_analytics_worker.get_update(action, item_id, paths=None, env=None)

@app.task
def run_python_function_base(params_url):
    '''Process the function and data submitted by the user.'''
    return celery_analytics_worker.run_python_function_base(params_url)

class CeleryAnalyticsWorker(AnalyticsWorker):
    '''Overloaded AnalyticsWorker to allow calling delayed functions. The
    delayed functions must be defined in the global scope (above)
    hence the overloaded functions call them with delay, which will
    then hook back into the default behaviour by calling the
    `original_*` function here.

    '''
    def run_python_function_subjob(self, url):
        '''Process a subjob, created by the base job.

        This method calls the original implementation through a celery
        delayed call.
        '''
        return run_python_function_subjob.delay(url)

    def original_run_python_function_subjob(self, url):
        '''Process a subjob, created by the base job.

        This method calls the original implementation in the parent
        class.
        '''
        return super().run_python_function_subjob(url)

    def monitor_jobs(self, decomposed, subjob_tasks, params_url):
        '''Monitors base job.

        This method calls the original implementation through a celery
        delayed call.
        '''
        return monitor_jobs.delay(decomposed, subjob_tasks, params_url)

    def original_monitor_jobs(self, decomposed, subjob_tasks, params_url):
        '''Monitors base job.

        This method calls the original implementation in the parent
        class.
        '''
        return super().monitor_jobs(decomposed, subjob_tasks, params_url)

# A CeleryAnalyticsWorker instance is created to allow inheritance of
# the base code.
celery_analytics_worker = CeleryAnalyticsWorker()


def make_rpc(dc_config):
    '''Return a new instance of Celery RPC.'''
    return RPCCelery(dc_config)
