"""Load project settings from a TOML file (stdlib only, no extra dependency)."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_CONFIG_PATH = Path("../config/sources.toml")
VALID_KINDS = {"monthly", "reference"}
VALID_FORMATS = {"parquet", "csv"}


class ConfigError(ValueError):
    """The configuration file is missing something or contains an invalid value."""


@dataclass(frozen=True)
class HttpSettings:
    timeout_seconds: float = 60.0
    max_attempts: int = 4
    backoff_base_seconds: float = 2.0
    not_available_statuses: frozenset[int] = frozenset({403, 404})
    user_agent: str = "nyc-taxi-data-platform/0.1 (learning project)"


@dataclass(frozen=True)
class SourceConfig:
    name: str
    kind: str  # "monthly" (one file per YYYY-MM) or "reference" (one static file)
    file_format: str  # "parquet" or "csv"
    url_template: str
    required_columns: tuple[str, ...] = ()

    def url_for(self, period: str | None = None) -> str:
        return self.url_template.format(period=period)


@dataclass(frozen=True)
class Settings:
    bronze_dir: Path
    http: HttpSettings = field(default_factory=HttpSettings)
    sources: dict[str, SourceConfig] = field(default_factory=dict)

    def source(self, name: str) -> SourceConfig:
        try:
            return self.sources[name]
        except KeyError:
            known = sorted(self.sources)
            raise ConfigError(f"unknown source {name!r}; available: {known}") from None


def _build_source(name: str, raw: dict) -> SourceConfig:
    for key in ("kind", "file_format", "url_template"):
        if key not in raw:
            raise ConfigError(f"source {name!r} is missing required key {key!r}")
    if raw["kind"] not in VALID_KINDS:
        raise ConfigError(f"source {name!r}: kind must be one of {sorted(VALID_KINDS)}")
    if raw["file_format"] not in VALID_FORMATS:
        raise ConfigError(f"source {name!r}: file_format must be one of {sorted(VALID_FORMATS)}")
    if raw["kind"] == "monthly" and "{period}" not in raw["url_template"]:
        raise ConfigError(f"source {name!r}: a monthly url_template must contain {{period}}")
    return SourceConfig(
        name=name,
        kind=raw["kind"],
        file_format=raw["file_format"],
        url_template=raw["url_template"],
        required_columns=tuple(raw.get("required_columns", ())),
    )


def load_settings(path: Path = DEFAULT_CONFIG_PATH) -> Settings:
    try:
        with open(path, "rb") as handle:
            raw = tomllib.load(handle)
    except FileNotFoundError:
        raise ConfigError(f"config file not found: {path}") from None
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"config file {path} is not valid TOML: {exc}") from exc

    http_raw = raw.get("http", {})
    defaults = HttpSettings()
    http = HttpSettings(
        timeout_seconds=float(http_raw.get("timeout_seconds", defaults.timeout_seconds)),
        max_attempts=int(http_raw.get("max_attempts", defaults.max_attempts)),
        backoff_base_seconds=float(
            http_raw.get("backoff_base_seconds", defaults.backoff_base_seconds)
        ),
        not_available_statuses=frozenset(
            http_raw.get("not_available_statuses", defaults.not_available_statuses)
        ),
        user_agent=http_raw.get("user_agent", defaults.user_agent),
    )
    sources = {name: _build_source(name, cfg) for name, cfg in raw.get("sources", {}).items()}
    if not sources:
        raise ConfigError(f"config file {path} defines no [sources.*] section")

    return Settings(
        bronze_dir=Path(raw.get("paths", {}).get("bronze_dir", "data/bronze")),
        http=http,
        sources=sources,
    )