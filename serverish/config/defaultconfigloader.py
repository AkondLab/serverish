import logging
from pathlib import Path

from dynaconf import Dynaconf
from dynaconf.utils.boxing import DynaBox

from serverish.messenger import single_read

_log = logging.getLogger('config')


class DefaultConfigLoader:
    """Loads configuration from YAML or TOML file."""

    def load_config_from_file(self, filename: str | Path) -> DynaBox:
        """Creates a Dynaconf instance and loads configuration from a given file."""
        filename = Path(filename)

        match filename.suffix:
            case '.yaml' | '.yml':
                config = DynaBox.from_yaml(filename=str(filename))
            case '.toml' | '.tml':
                config = DynaBox.from_toml(filename=str(filename))
            case _:
                raise ValueError(f"Unsupported file format: {filename.suffix}")
        return config

    def update_config_from_file(self, config: DynaBox| dict, filename: str | Path) -> None:
        """Recursively updates an existing Dynaconf instance with values from a new file."""

        config = DynaBox(config)
        new_config = self.load_config_from_file(filename)
        config.update(new_config, merge=True)

    async def load_config_from_messenger(self, subject: str = 'site.config',
                                   fallbackfile: str | Path | None = None) -> DynaBox:
        """Loads configuration from a messenger subject."""
        try:
            cfg = await single_read(subject)
        except Exception as e:
            _log.error(f"Failed to read config from messenger subject {subject}, {e}")
            if not fallbackfile:
                cfg = {}
            else:
                _log.info("Trying to load fallback config from %s", fallbackfile)
                try:
                    cfg = self.load_config_from_file(fallbackfile)
                except Exception as e:
                    _log.error(f"Failed to load fallback config")
                    cfg = {}
        return DynaBox(cfg)
