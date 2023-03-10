from ..client import queries
from . import config, models


def aggregate_filetypes(
    path: str,
    elastic_config: config.ElasticSchema,
    volume_info: models.Volume,
) -> list[models.GranularRecord]:
    filetypes = []
    filetypes_q = queries.filetypes(path, elastic_config["data_index_name"], volume_info.meta.id)
    for type_ in filetypes_q["aggregations"]["counts"].keys():
        if type_ != "doc_count" and (filetypes_q["aggregations"]["counts"][type_]["value"] > 0):
            record = models.GranularRecord(
                path=path,
                scan_id=volume_info.meta.id,
                category="filetypes",
                identifier=type_,
                size=filetypes_q["aggregations"]["sizes"][type_]["value"],
                count=filetypes_q["aggregations"]["counts"][type_]["value"],
                start_timestamp=volume_info.start_timestamp,
                end_timestamp=volume_info.end_timestamp,
            )
            filetypes.append(record.to_dict())
    return filetypes


def aggregate_users(
    path: str,
    elastic_config: config.ElasticSchema,
    volume_info: models.Volume,
) -> list[models.GranularRecord]:
    users = []
    users_q = queries.users(path, elastic_config["data_index_name"], volume_info.meta.id)
    for user in users_q["aggregations"]["counts"].keys():
        if user != "doc_count" and (users_q["aggregations"]["counts"][user]["value"] > 0):
            record = models.GranularRecord(
                path=path,
                scan_id=volume_info.meta.id,
                category="users",
                identifier=user,
                size=users_q["aggregations"]["sizes"][user]["value"],
                count=users_q["aggregations"]["counts"][user]["value"],
                start_timestamp=volume_info.start_timestamp,
                end_timestamp=volume_info.end_timestamp,
            )
            users.append(record.to_dict())
    return users


def aggregate_heat(
    path: str,
    elastic_config: config.ElasticSchema,
    volume_info: models.Volume,
) -> list[models.GranularRecord]:
    heat_bins = []
    heat_q = queries.hotness(path, elastic_config["data_index_name"], volume_info.meta.id)
    for hot in heat_q["aggregations"]["counts"].keys():
        if hot != "doc_count" and (heat_q["aggregations"]["counts"][hot]["value"] > 0):
            record = models.GranularRecord(
                path=path,
                scan_id=volume_info.meta.id,
                category="heat_bins",
                identifier=hot,
                size=heat_q["aggregations"]["sizes"][hot]["value"],
                count=heat_q["aggregations"]["counts"][hot]["value"],
                start_timestamp=volume_info.start_timestamp,
                end_timestamp=volume_info.end_timestamp,
            )
            heat_bins.append(record.to_dict())
    return heat_bins
