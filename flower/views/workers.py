import asyncio
import logging
import time
from collections import defaultdict

from tornado import web

from ..options import options
from ..utils.broker import Broker
from ..views import BaseHandler

logger = logging.getLogger(__name__)


class WorkerView(BaseHandler):
    @web.authenticated
    async def get(self, name):
        try:
            futures = self.application.update_workers(workername=name)
            if futures:
                await asyncio.gather(*futures, return_exceptions=True)
        except Exception as e:
            logger.error(e)

        worker = self.application.workers.get(name)

        if worker is None:
            raise web.HTTPError(404, f"Unknown worker '{name}'")
        if 'stats' not in worker:
            raise web.HTTPError(404, f"Unable to get stats for '{name}' worker")

        self.render("worker.html", worker=dict(worker, name=name))


class WorkersView(BaseHandler):
    @web.authenticated
    async def get(self):
        refresh = self.get_argument('refresh', default=False, type=bool)
        json = self.get_argument('json', default=False, type=bool)

        events = self.application.events.state

        # Await inspector so active_queues data is available before rendering
        try:
            futures = self.application.update_workers()
            if futures:
                await asyncio.gather(*futures, return_exceptions=True)
        except Exception as e:
            logger.exception('Failed to update workers: %s', e)

        workers = {}
        for name, values in events.counter.items():
            if name not in events.workers:
                continue
            worker = events.workers[name]
            info = dict(values)
            info.update(self._as_dict(worker))
            info.update(status=worker.alive)
            workers[name] = info

        if options.purge_offline_workers is not None:
            timestamp = int(time.time())
            offline_workers = []
            for name, info in workers.items():
                if info.get('status', True):
                    continue

                heartbeats = info.get('heartbeats', [])
                last_heartbeat = int(max(heartbeats)) if heartbeats else None
                if not last_heartbeat or timestamp - last_heartbeat > options.purge_offline_workers:
                    offline_workers.append(name)

            for name in offline_workers:
                workers.pop(name)

        # Fetch queue lengths from the broker and attach to each worker
        try:
            app = self.application
            http_api = None
            if app.transport == 'amqp' and app.options.broker_api:
                http_api = app.options.broker_api

            broker = Broker(app.capp.connection(connect_timeout=1.0).as_uri(include_password=True),
                            http_api=http_api, broker_options=self.capp.conf.broker_transport_options,
                            broker_use_ssl=self.capp.conf.broker_use_ssl)

            all_queue_names = self.get_active_queue_names()
            queue_stats = await broker.queues(all_queue_names)
            queue_lengths = {q['name']: q.get('messages', 0) for q in queue_stats}

            for name, info in workers.items():
                worker_info = app.workers.get(name, {})
                worker_queues = [q['name'] for q in worker_info.get('active_queues', [])]
                info['queue_length'] = sum(queue_lengths.get(q, 0) for q in worker_queues)
        except Exception as e:
            logger.error("Failed to fetch queue lengths: %s", e)
            for info in workers.values():
                info.setdefault('queue_length', 0)

        # Compute per-queue average response times
        queue_tasks = defaultdict(list)
        for task_id, task in events.tasks.items():
            if task.state == 'SUCCESS' and task.received and task.succeeded:
                queue_name = task.routing_key or 'default'
                response_time = task.succeeded - task.received
                queue_tasks[queue_name].append((task.succeeded, response_time))

        queue_response_times = {}
        for queue_name, tasks in queue_tasks.items():
            tasks.sort(key=lambda t: t[0], reverse=True)
            times = [t[1] for t in tasks]
            queue_response_times[queue_name] = {}
            for n in (10, 100, 1000):
                subset = times[:n]
                if subset:
                    queue_response_times[queue_name][n] = round(sum(subset) / len(subset), 2)
                else:
                    queue_response_times[queue_name][n] = None

        for name, info in workers.items():
            worker_info = self.application.workers.get(name, {})
            worker_queues = [q['name'] for q in worker_info.get('active_queues', [])]
            for n in (10, 100, 1000):
                avgs = [queue_response_times[q][n]
                        for q in worker_queues
                        if q in queue_response_times and queue_response_times[q][n] is not None]
                if avgs:
                    info[f'avg_response_{n}'] = round(sum(avgs) / len(avgs), 2)
                else:
                    info[f'avg_response_{n}'] = None

        if json:
            self.write(dict(data=list(workers.values())))
        else:
            self.render("workers.html",
                        workers=workers,
                        broker=self.application.capp.connection().as_uri(),
                        autorefresh=1 if self.application.options.auto_refresh else 0)

    @classmethod
    def _as_dict(cls, worker):
        if hasattr(worker, '_fields'):
            return dict((k, getattr(worker, k)) for k in worker._fields)
        return cls._info(worker)

    @classmethod
    def _info(cls, worker):
        _fields = ('hostname', 'pid', 'freq', 'heartbeats', 'clock',
                   'active', 'processed', 'loadavg', 'sw_ident',
                   'sw_ver', 'sw_sys')

        def _keys():
            for key in _fields:
                value = getattr(worker, key, None)
                if value is not None:
                    yield key, value

        return dict(_keys())
