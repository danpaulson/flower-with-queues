import logging
import time

from tornado import web

from ..options import options
from ..views import BaseHandler

logger = logging.getLogger(__name__)


class WorkerView(BaseHandler):
    list_limit = 50

    @web.authenticated
    async def get(self, name):
        try:
            update = self.application.update_workers(workername=name)
            # wait for inspection only when the cache lacks something this page shows
            # cached workers render immediately and refresh in the background
            cached = self.application.workers.get(name, {})
            if any(method not in cached for method in self.application.inspector.inspect_methods):
                await update
        except Exception as e:
            logger.error(e)

        worker = self.application.workers.get(name)

        if worker is None:
            raise web.HTTPError(404, f"Unknown worker '{name}'")
        if 'stats' not in worker:
            raise web.HTTPError(404, f"Unable to get stats for '{name}' worker")

        worker = dict(worker, name=name)
        # Scheduled and revoked lists can hold thousands of entries, show the head
        limit = self.get_argument('limit', self.list_limit, type=int)
        for key, value in worker.items():
            if isinstance(value, list):
                worker[key] = value[:limit]

        self.render(
            "worker.html",
            worker=worker,
            read_only=self.application.options.read_only,
        )


class WorkersView(BaseHandler):
    @web.authenticated
    async def get(self):
        refresh = self.get_argument('refresh', default=False, type=bool)
        json = self.get_argument('json', default=False, type=bool)

        events = self.application.events.state

        if refresh:
            try:
                self.application.update_workers()
            except Exception:
                logger.exception('Failed to update workers')

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
                last_seen = max(heartbeats) if heartbeats else \
                    getattr(events.workers[name], 'timestamp', None)
                if not last_seen or timestamp - int(last_seen) >= options.purge_offline_workers:
                    offline_workers.append(name)

            for name in offline_workers:
                workers.pop(name)

        await self._attach_queue_lengths(workers)

        if json:
            self.write({"data": list(workers.values())})
        else:
            self.render("workers.html",
                        workers=workers,
                        broker=self.application.broker_uri,
                        autorefresh=1 if self.application.options.auto_refresh else 0)

    async def _attach_queue_lengths(self, workers):
        """The Queue column: pending messages on the queues each worker consumes.

        Reads the queue names off the CACHED inspector data (``active_queues``,
        filled by the periodic / on-refresh inspect) and counts messages on the
        broker directly — one cheap broker call per poll, and never an inspect
        broadcast: the previous fork inspected every node on every one-second
        poll, which exhausted the broker connection pool within minutes and hung
        every inspect behind it (swgoh.gg, 2026-09-26).
        """
        try:
            queue_lengths = {}
            names = self.get_active_queue_names()
            if names:
                stats = await self.get_broker().queues(names)
                queue_lengths = {q['name']: q.get('messages', 0) or 0 for q in stats}
            for name, info in workers.items():
                consumed = [q['name'] for q in self.application.workers.get(name, {}).get('active_queues', [])]
                info['queue_length'] = sum(queue_lengths.get(q, 0) for q in consumed)
        except Exception as e:
            logger.error("Failed to fetch queue lengths: %s", e)
            for info in workers.values():
                info.setdefault('queue_length', 0)

    @classmethod
    def _as_dict(cls, worker):
        return {k: getattr(worker, k) for k in worker._fields}
