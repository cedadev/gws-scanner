"""Module which sends data to elasticsearch."""
import datetime as dt
import multiprocessing as mp
import queue as queue_
import threading as th
import typing

import elasticsearch.helpers as esh
import elasticsearch_dsl as esd

from . import config, models, util


def init(elastic_config: config.ElasticSchema) -> None:
    """Initialise elasticsearch."""
    conn = get_connection(elastic_config)

    ensure_index(
        elastic_config["data_index_name"],
        models.File,
        conn,
        index_settings={"mapping.total_fields.limit": 10000},
    )
    ensure_index(elastic_config["volume_index_name"], models.Volume, conn)
    ensure_index(elastic_config["aggregate_index_name"], models.GranularRecord, conn)


def ensure_index(
    index_name: str,
    documentclass: typing.Type[esd.Document],
    conn: typing.Any,
    index_settings: typing.Optional[dict[str, typing.Any]] = None,
) -> None:
    """Initialise es indexes and associate documents with them."""
    index = esd.Index(index_name)
    if index_settings:
        index.settings(**index_settings)
    index.document(documentclass)

    # Create, or update the template index, including any new mappings or settings.
    # Note that this will not update the current index- must run `migrate` for that.
    index_template = index.as_template(index_name, f"{index_name}-*")
    index_template.save()

    # If the alias doesn't exist, create it.
    # This happens on the first run only.
    if not index.exists():
        migrate(conn, index_name, move_data=False)


def migrate(
    conn: typing.Any, alias: str, move_data: bool = True, update_alias: bool = True
) -> None:
    """Create or migrate the index in elasticsearch.

    This is expensive and should not happen on every run, only if the mappings
    have changed.
    """
    # Name of the new index.
    next_index = f"{alias}-{dt.datetime.now().strftime('%Y%m%d%H%M%S%f')}"

    # Create a new index, will automatically use settings and mappings from template.
    conn.indices.create(index=next_index)

    # Move data from the old index into the new one.
    if move_data:
        conn.reindex(
            body={"source": {"index": alias}, "dest": {"index": next_index}},
            request_timeout=3600,
        )
        conn.indices.refresh(index=next_index)

    # Update the alias to point to the new index.
    if update_alias:
        conn.indices.update_aliases(
            body={
                "actions": [
                    {"remove": {"alias": alias, "index": f"{alias}-*"}},
                    {"add": {"alias": alias, "index": next_index}},
                ]
            }
        )


def get_connection(config_: config.ElasticSchema) -> typing.Any:
    """Create a connection to elasticsearch."""
    esd.connections.create_connection(
        alias="default",
        hosts=config_["hosts"],
        use_ssl=config_["use_ssl"],
        timeout=config_["timeout"],
        retry_on_timeout=True,
        max_retries=10,
        headers={"x-api-key": config_["api_key"]},
    )
    return esd.connections.get_connection()


def worker(
    inqueue: queue_.Queue[models.File],
    config_: config.ScannerSchema,
    shutdown: th.Event,
) -> None:
    """Consumes Folder objects and send them to elasticsearch."""
    proc = mp.current_process()
    proc.name = f"elastic-{proc.name}"

    elastic_config = config_["elastic"]

    get_connection(elastic_config)
    connection = esd.connections.get_connection()

    staging: list[models.File] = []
    while (not shutdown.is_set()) or (len(staging) > 0):
        send = False
        try:
            staging.append(inqueue.get(timeout=10))
            inqueue.task_done()
        except queue_.Empty:
            send = True
        if len(staging) > 0:
            if (len(staging) >= max(1000, config_["queue_length_scale_factor"])) or send:
                esh.bulk(
                    connection,
                    staging,
                    index=elastic_config["data_index_name"],
                )
                staging.clear()
