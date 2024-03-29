import logging
import logging.config
import logging.handlers
import multiprocessing as mp
import multiprocessing.context
import multiprocessing.queues
import multiprocessing.synchronize
import queue as queue_
import threading as th
import typing

from .. import constants
from . import config, elastic, errors, models, scanner

T = typing.TypeVar("T")


class CancellableQueue(queue_.Queue[T]):
    """Create a cancellable Queue."""

    def __init__(self, maxsize: int = 0, *, abort_event: th.Event):
        self.abort_event = abort_event
        super().__init__(maxsize=maxsize)

    def join(self) -> None:
        """Override logic from queue join method.

        Follows the same logic as
        https://github.com/python/cpython/blob/main/Lib/queue.py#L79
        but allows the queue to exit when signalled with an error.
        """
        with self.all_tasks_done:
            while self.unfinished_tasks:
                if self.abort_event.is_set():
                    return
                self.all_tasks_done.wait(5)


# The type of this should be multiprocessing.queues.JoinableQueue[T] but that breaks at runtime.
class CancellableJoinableQueue(multiprocessing.queues.JoinableQueue):  # type: ignore[type-arg]
    """Create a cancellable multiprocessing Queue."""

    def __init__(
        self,
        maxsize: int = 0,
        *,
        ctx: multiprocessing.context.BaseContext,
        abort_event: multiprocessing.synchronize.Event,
    ):
        self.abort_event = abort_event
        super().__init__(maxsize=maxsize, ctx=ctx)

    def join(self) -> None:
        """Override logic from multiprocessing queue join method.

        Follows the same logic as
        https://github.com/python/cpython/blob/main/Lib/multiprocessing/queues.py#L330
        but allows the queue to exit when signalled with an error.
        """
        # This errors probably relate to the whole class not being typed properly
        with self._cond:  # type: ignore[attr-defined]
            while not self._unfinished_tasks._semlock._is_zero():  # type: ignore[attr-defined]
                if self.abort_event.is_set():
                    return
                self._cond.wait(5)  # type: ignore[attr-defined]


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
        abort: multiprocessing.synchronize.Event,
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


class QueueLogger:
    def __init__(self, name: typing.Optional[str] = None, *, log_config: dict[str, typing.Any]):
        log_config = constants.DEFAULT_LOGGING_CONFIG | log_config
        self.queue: queue_.Queue[typing.Any] = mp.Queue()
        logging.config.dictConfig(log_config)
        logger = logging.getLogger()
        self.listener = logging.handlers.QueueListener(self.queue, *logger.handlers)
        self.listener.start()

    def shutdown(self) -> None:
        self.listener.stop()


def getLogger(
    name: typing.Optional[str] = None, *, queue: queue_.Queue[typing.Any]
) -> logging.Logger:
    """Return a logger which will pass all items up to a QueueHandler."""
    queue_handler = logging.handlers.QueueHandler(queue)
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.addHandler(queue_handler)
    return logger


class FilterNotify(logging.Filter):
    """Log filter to filter only records where level==100"""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno == 100:
            return True
        return False
