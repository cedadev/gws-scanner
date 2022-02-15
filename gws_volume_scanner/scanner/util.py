import multiprocessing as mp
import queue as queue_

from . import config, elastic, models, scanner


class ElasticQueueWorker:
    """Create and manage a worker for sending files to es."""

    def __init__(self, config_: config.ScannerSchema):

        # Used to signal to the worker to exit.
        self._shutdown = mp.Event()

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
        self, config_: config.ScannerSchema, elastic_q: queue_.Queue[models.File]
    ):

        # Used to signal to the worker to exit.
        self._shutdown = mp.Event()

        # Setup queue of items for the scanner.
        self.queue: mp.JoinableQueue[scanner.ToScan] = mp.JoinableQueue(
            config_["scan_processes"] * config_["queue_length_scale_factor"]
        )

        # Pool of workers to deal with the queue.
        self._pl = mp.Pool(  # pylint: disable=consider-using-with
            processes=config_["scan_processes"],
            initializer=scanner.worker,
            initargs=((self.queue, elastic_q, config_, self._shutdown)),
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
