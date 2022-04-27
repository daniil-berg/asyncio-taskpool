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
CLI entry point script for a :class:`ControlClient`.
"""


from argparse import ArgumentParser
from asyncio import run
from pathlib import Path
from typing import Any, Dict, Sequence

from ..internals.constants import PACKAGE_NAME
from ..pool import TaskPool
from .client import TCPControlClient, UnixControlClient
from .server import TCPControlServer, UnixControlServer


__all__ = []


CLIENT_CLASS = 'client_class'
UNIX, TCP = 'unix', 'tcp'
SOCKET_PATH = 'socket_path'
HOST, PORT = 'host', 'port'


def parse_cli(args: Sequence[str] = None) -> Dict[str, Any]:
    parser = ArgumentParser(
        prog=f'{PACKAGE_NAME}.control',
        description=f"Simple CLI based control client for {PACKAGE_NAME}"
    )
    subparsers = parser.add_subparsers(title="Connection types")

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
    tcp_parser.set_defaults(**{CLIENT_CLASS: TCPControlClient})

    unix_parser = subparsers.add_parser(UNIX, help="Connect via unix socket")
    unix_parser.add_argument(
        SOCKET_PATH,
        type=Path,
        help=f"Path to the unix socket on which the {UnixControlServer.__name__} for the {TaskPool.__name__} is "
             f"listening."
    )
    unix_parser.set_defaults(**{CLIENT_CLASS: UnixControlClient})

    return vars(parser.parse_args(args))


async def main():
    kwargs = parse_cli()
    client_cls = kwargs.pop(CLIENT_CLASS)
    await client_cls(**kwargs).start()


if __name__ == '__main__':
    run(main())
