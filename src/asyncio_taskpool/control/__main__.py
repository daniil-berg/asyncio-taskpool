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

from ..constants import PACKAGE_NAME
from ..pool import TaskPool
from .client import ControlClient, TCPControlClient, UnixControlClient
from .server import TCPControlServer, UnixControlServer


CONN_TYPE = 'conn_type'
UNIX, TCP = 'unix', 'tcp'
SOCKET_PATH = 'path'
HOST, PORT = 'host', 'port'


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
        help=f"Path to the unix socket on which the {UnixControlServer.__name__} for the {TaskPool.__name__} is "
             f"listening."
    )
    tcp_parser = subparsers.add_parser(TCP, help="Connect via TCP socket")
    tcp_parser.add_argument(
        HOST,
        help=f"IP address or url that the {TCPControlServer.__name__} for the {TaskPool.__name__} is listening on."
    )
    tcp_parser.add_argument(
        PORT,
        type=int,
        help=f"Port that the {TCPControlServer.__name__} for the {TaskPool.__name__} is listening on."
    )
    return vars(parser.parse_args())


async def main():
    kwargs = parse_cli()
    if kwargs[CONN_TYPE] == UNIX:
        client = UnixControlClient(socket_path=kwargs[SOCKET_PATH])
    elif kwargs[CONN_TYPE] == TCP:
        client = TCPControlClient(host=kwargs[HOST], port=kwargs[PORT])
    else:
        print("Invalid connection type", file=sys.stderr)
        sys.exit(2)
    await client.start()

if __name__ == '__main__':
    run(main())
