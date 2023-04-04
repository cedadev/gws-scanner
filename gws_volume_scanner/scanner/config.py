"""GWS Scanner configuration loading and parsing."""
import os
import typing

import tomli
import typeguard

from . import errors


## The following classes setup the shape of the configuration files.
## They are used both for type checking AND AT RUNTIME to check if the config is valid.
class ElasticSchema(typing.TypedDict):
    """Schema for settings for elastic."""

    data_index_name: str
    volume_index_name: str
    aggregate_index_name: str

    ca_certs: str
    use_ssl: bool
    hosts: typing.List[str]
    timeout: int
    api_key: str


class DaemonSchema(typing.TypedDict):
    """Schema for the scanner daemon."""

    fail_threshold: int

    client_id: str
    client_secret: str
    scopes: list[str]
    token_endpoint: str
    services_endpoint: str

    max_scan_interval_days: int

    extra_to_scan: list[str]
    never_scan: list[str]


class ScannerSchema(typing.TypedDict):
    """Schema for configuration of the sanner itself."""

    scan_processes: int
    scan_max_threads_per_process: int

    queue_length_scale_factor: int

    daemon: DaemonSchema
    elastic: ElasticSchema


class GWSConfigSchema(typing.TypedDict, total=False):
    """Schema for settings for an invididual GWS."""

    full_item_walk_dirs: typing.List[str]
    aggregate_subdir_paths: typing.List[str]
    aggregate_subdir_names: typing.List[str]
    scan_depth: int


class GWSMainConfigSchema(typing.TypedDict, total=False):
    """Schema for the GWS section of the main configuration file."""

    defaults: GWSConfigSchema
    overrides: GWSConfigSchema
    configs: typing.Dict[str, GWSConfigSchema]


class MainConfigSchema(typing.TypedDict):
    """Schema for the whole main configuration file."""

    scanner: ScannerSchema
    gws: GWSMainConfigSchema


## This class is the main configuration object for the scanner.
class ScannerConfig:
    """Object holding the configuration for the GWS Scanner."""

    def __init__(self, path: str) -> None:
        """Load the main scanner config file and check it's validity."""
        with open(path, "rb") as thefile:
            toml_dict = typing.cast(MainConfigSchema, tomli.load(thefile))

        # Check that the scanner config is what we expect.
        try:
            typeguard.check_type("config", toml_dict, MainConfigSchema)
        except TypeError as err:
            raise errors.ScannerMainConfigError from err

        # Save items into the config object.
        try:
            # Required.
            self._scanner_config = toml_dict["scanner"]
            self._gws_defaults = toml_dict["gws"]["defaults"]
            self._gws_overrides = toml_dict["gws"]["overrides"]
            # Allowed to be empty.
            self._gws_configs = toml_dict["gws"].get("configs", {})
        except KeyError as err:
            raise errors.ScannerMainConfigError from err

    def gws_config(self, path: str) -> GWSConfigSchema:
        """Get the configuration for a single GWS."""
        configpath = os.path.join(path, ".gws-scanner-config.toml")

        # Get the user's configuration.
        user_dict = GWSConfigSchema()
        if os.path.exists(configpath):
            with open(configpath, "rb") as thefile:
                user_dict = typing.cast(GWSConfigSchema, tomli.load(thefile))
            try:
                typeguard.check_type("config", user_dict, GWSConfigSchema)
            except TypeError as err:
                raise errors.ScannerGWSConfigError from err

        # Combine it with the default.
        # Mypy does not understand **. When we get 3.9 and can | dicts this will be better.
        config_dict = typing.cast(GWSConfigSchema, {**self._gws_defaults, **user_dict})

        # Get any config from the admin configuration.
        config_dict = typing.cast(
            GWSConfigSchema, {**config_dict, **self._gws_configs.get(path, {})}
        )

        # Apply overrides.
        config_dict["full_item_walk_dirs"] = list(
            set(
                config_dict["full_item_walk_dirs"]
                + self._gws_overrides.get("full_item_walk_dirs", [])
            )
        )
        config_dict["aggregate_subdir_paths"] = list(
            set(
                config_dict["aggregate_subdir_paths"]
                + self._gws_overrides.get("aggregate_subdir_paths", [])
            )
        )
        config_dict["aggregate_subdir_names"] = list(
            set(
                config_dict["aggregate_subdir_names"]
                + self._gws_overrides.get("aggregate_subdir_names", [])
            )
        )
        config_dict["scan_depth"] = min(
            config_dict["scan_depth"], self._gws_overrides.get("scan_depth", 100000)
        )
        return config_dict

    @property
    def scanner(self) -> ScannerSchema:
        """Scanner configuration."""
        return self._scanner_config
