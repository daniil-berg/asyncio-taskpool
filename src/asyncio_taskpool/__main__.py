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
