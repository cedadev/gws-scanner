import multiprocessing as mp
import multiprocessing.queues as mpq
import multiprocessing.synchronize as mps
import queue as queue_
import threading as th
import typing

from . import config, elastic, errors, models, scanner


class CancellableQueue(queue_.Queue):
    def __init__(self, *args, abort_event: th.Event, **kwargs):
        self.abort_event = abort_event
        super().__init__(*args, **kwargs)

    def join(self):
        with self.all_tasks_done:
            while self.unfinished_tasks:
                if self.abort_event.is_set():
                    raise errors.AbortError
                else:
                    self.all_tasks_done.wait(30)


class CancellableJoinableQueue(mpq.JoinableQueue):
    def __init__(self, *args, abort_event: mps.Event, **kwargs):
        self.abort_event = abort_event
        super().__init__(*args, **kwargs)

    def join(self):
        with self._cond:
            if self.abort_event.is_set():
                raise errors.AbortError
            elif not self._unfinished_tasks._semlock._is_zero():
                self._cond.wait(30)


class ElasticQueueWorker:
    """Create and manage a worker for sending files to es."""

    def __init__(self, config_: config.ScannerSchema):
        # Used to signal to the worker to exit.
        self._shutdown = mp.Event()
        self._abort = mp.Event()

        # Setup queue of items for elasticsearch.
        self.queue: mp.JoinableQueue[models.File] = mp.JoinableQueue(
            config_["queue_length_scale_factor"]
        )

        # Start process to to elastic tasks.
        self._pr = mp.Process(
            target=elastic.worker,
            args=((self.queue, config_, self._shutdown)),
        )
        self._pr.start()

    def shutdown(self) -> None:
        """Shutdown queue and worker and make sure everything gets tidied up."""
        # Ensure queue is done.
        self.queue.close()
        self.queue.join()

        # Signal to worker it should finish.
        self._shutdown.set()

        # Shut the process down.
        self._pr.join()
        self._pr.close()

        # Shutdown queue completely.
        self.queue.join_thread()


class ScanQueueWorker:
    """Create and mannage queue and worker pool for scanning files."""

    def __init__(
        self,
        config_: config.ScannerSchema,
        elastic_q: queue_.Queue[models.File],
        abort: mps.Event,
    ):
        # Used to signal to the worker to exit.
        self._shutdown = mp.Event()
        self._abort = abort

        # Setup queue of items for the scanner.
        ctx = mp.get_context()
        self.queue: mp.JoinableQueue[scanner.ToScan] = CancellableJoinableQueue(
            config_["scan_processes"] * config_["queue_length_scale_factor"],
            abort_event=self._abort,
            ctx=ctx,
        )

        # Pool of workers to deal with the queue.
        self._pl = mp.Pool(  # pylint: disable=consider-using-with
            processes=config_["scan_processes"],
            initializer=scanner.worker,
            initargs=((self.queue, elastic_q, config_, self._shutdown, self._abort)),
        )

    def shutdown(self) -> None:
        """Shutdown queue and worker pool and make sure everything gets tidied up."""
        # Ensure queue is done.
        self.queue.close()
        self.queue.join()

        # Signal to worker it should finish.
        self._shutdown.set()

        # Ensure pool is done.
        # The order of these is different to if you are using a worker.
        self._pl.close()
        self._pl.join()

        # Shutdown queue completely.
        self.queue.join_thread()
