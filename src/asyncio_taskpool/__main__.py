__author__ = "Daniil Fajnberg"
__copyright__ = "Copyright © 2022 Daniil Fajnberg"
__license__ = """GNU LGPLv3.0

This file is part of asyncio-taskpool.

asyncio-taskpool is free software: you can redistribute it and/or modify it under the terms of
version 3.0 of the GNU Lesser General Public License as published by the Free Software Foundation.

asyncio-taskpool is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
See the GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along with asyncio-taskpool. 
If not, see <https://www.gnu.org/licenses/>."""

__doc__ = """
CLI client entry point.
"""


import sys
from argparse import ArgumentParser
from asyncio import run
from pathlib import Path
from typing import Dict, Any

from .client import ControlClient, UnixControlClient
from .constants import PACKAGE_NAME
from .pool import TaskPool
from .server import ControlServer


CONN_TYPE = 'conn_type'
UNIX, TCP = 'unix', 'tcp'
SOCKET_PATH = 'path'


def parse_cli() -> Dict[str, Any]:
    parser = ArgumentParser(
        prog=PACKAGE_NAME,
        description=f"CLI based {ControlClient.__name__} for {PACKAGE_NAME}"
    )
    subparsers = parser.add_subparsers(title="Connection types", dest=CONN_TYPE)
    unix_parser = subparsers.add_parser(UNIX, help="Connect via unix socket")
    unix_parser.add_argument(
        SOCKET_PATH,
        type=Path,
        help=f"Path to the unix socket on which the {ControlServer.__name__} for the {TaskPool.__name__} is listening."
    )
    return vars(parser.parse_args())


async def main():
    kwargs = parse_cli()
    if kwargs[CONN_TYPE] == UNIX:
        client = UnixControlClient(path=kwargs[SOCKET_PATH])
    elif kwargs[CONN_TYPE] == TCP:
        # TODO: Implement the TCP client class
        client = UnixControlClient(path=kwargs[SOCKET_PATH])
    else:
        print("Invalid connection type", file=sys.stderr)
        sys.exit(2)
    await client.start()

if __name__ == '__main__':
    run(main())
