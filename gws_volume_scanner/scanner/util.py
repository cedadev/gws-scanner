import multiprocessing as mp
import multiprocessing.queues as mpq
import multiprocessing.synchronize as mps
import queue as queue_
import threading as th
import typing

from . import config, elastic, errors, models, scanner


class CancellableQueue(queue_.Queue):
    """Create a cancellable Queue."""

    def __init__(self, *args, abort_event: th.Event, **kwargs):
        self.abort_event = abort_event
        super().__init__(*args, **kwargs)

    def join(self):
        """Override logic from queue join method.

        Follows the same logic as
        https://github.com/python/cpython/blob/main/Lib/queue.py#L79
        but allows the queue to exit when signalled with an error.
        """
        with self.all_tasks_done:
            while self.unfinished_tasks:
                if self.abort_event.is_set():
                    raise errors.AbortError()
                self.all_tasks_done.wait(30)


class CancellableJoinableQueue(mpq.JoinableQueue):
    """Create a cancellable multiprocessing Queue."""

    def __init__(self, *args, abort_event: mps.Event, **kwargs):
        self.abort_event = abort_event
        super().__init__(*args, **kwargs)

    def join(self):
        """Override logic from multiprocessing queue join method.

        Follows the same logic as
        https://github.com/python/cpython/blob/main/Lib/multiprocessing/queues.py#L330
        but allows the queue to exit when signalled with an error.
        """
        with self._cond:
            while not self._unfinished_tasks._semlock._is_zero():
                if self.abort_event.is_set():
                    raise errors.AbortError()
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
