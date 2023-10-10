"""CLI entry point script for a :class:`ControlClient`."""
from __future__ import annotations

from argparse import ArgumentParser
from asyncio import run
from pathlib import Path
from typing import Any, Dict, Sequence

from ..internals.constants import PACKAGE_NAME
from ..pool import TaskPool
from .client import TCPControlClient, UnixControlClient
from .server import TCPControlServer, UnixControlServer

CLIENT_CLASS = "client_class"
UNIX, TCP = "unix", "tcp"
SOCKET_PATH = "socket_path"
HOST, PORT = "host", "port"


def parse_cli(args: Sequence[str] | None = None) -> Dict[str, Any]:
    """Parses command line arguments returning the resulting namespace."""
    parser = ArgumentParser(
        prog=f"{PACKAGE_NAME}.control",
        description=f"Simple CLI based control client for {PACKAGE_NAME}",
    )
    subparsers = parser.add_subparsers(title="Connection types")

    tcp_parser = subparsers.add_parser(TCP, help="Connect via TCP socket")
    tcp_parser.add_argument(
        HOST,
        help=f"IP address or url that the {TCPControlServer.__name__} "
        f"for the {TaskPool.__name__} is listening on.",
    )
    tcp_parser.add_argument(
        PORT,
        type=int,
        help=f"Port that the {TCPControlServer.__name__} "
        f"for the {TaskPool.__name__} is listening on.",
    )
    tcp_parser.set_defaults(**{CLIENT_CLASS: TCPControlClient})

    unix_parser = subparsers.add_parser(UNIX, help="Connect via unix socket")
    unix_parser.add_argument(
        SOCKET_PATH,
        type=Path,
        help=f"Path to the unix socket on which "
        f"the {UnixControlServer.__name__} for the {TaskPool.__name__} "
        f"is listening.",
    )
    unix_parser.set_defaults(**{CLIENT_CLASS: UnixControlClient})

    return vars(parser.parse_args(args))


async def main() -> None:
    """
    Main entrypoint for the CLI script.

    Parses the command line arguments and starts the appropriate client.
    """
    kwargs = parse_cli()
    client_cls = kwargs.pop(CLIENT_CLASS)
    await client_cls(**kwargs).start()


if __name__ == "__main__":
    run(main())
