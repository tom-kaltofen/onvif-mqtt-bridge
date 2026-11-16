#!/usr/bin/env python3
"""ONVIF-MQTT bridge - entry point.

Runs directly as a script (no package, no dots, no `-m`):

    ./onvif_mqtt_bridge.py --config config.yaml
    python3 onvif_mqtt_bridge.py --config config.yaml

Python always adds the directory of the script being run to `sys.path[0]`,
so the flat, dot-free imports in `bridge.py`/`config.py`/etc. (`from config
import BridgeConfig`, not `from .config import BridgeConfig`) resolve
correctly no matter which directory this is invoked from - important for
running it as a systemd service with an absolute `ExecStart` path (see
`systemd/onvif-mqtt-bridge.service` and the "Running as a systemd service"
section in README.md).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from bridge import Bridge
from config import BridgeConfig
from const import LOGGER

__version__ = "1.0.0"


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="onvif_mqtt_bridge.py",
        description="Forwards ONVIF camera events as MQTT messages.",
    )
    parser.add_argument(
        "--config",
        "-c",
        default="config.yaml",
        help="Path to the YAML configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging (including onvif/zeep)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"onvif-mqtt-bridge {__version__}",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    if not args.verbose:
        # onvif/zeep are very chatty at INFO level.
        logging.getLogger("onvif").setLevel(logging.WARNING)
        logging.getLogger("zeep").setLevel(logging.WARNING)

    try:
        config = BridgeConfig.from_file(args.config)
    except (OSError, ValueError) as err:
        LOGGER.error("Could not load configuration: %s", err)
        return 1

    bridge = Bridge(config)
    try:
        asyncio.run(bridge.async_run_forever())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
