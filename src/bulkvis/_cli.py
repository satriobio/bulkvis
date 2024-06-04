"""Main entry point for command line read until scripts.

Set as entrypoint in ``pyproject.toml``
"""

from bulkvis.main import *

def main(argv: list[str] | None = None) -> None:
    """
    """
    parser = argparse.ArgumentParser(
        prog="satudna",
        epilog="See '<command> --help' to read about a specific sub-command.",
        allow_abbrev=False,
    )
    version = f"3.0"
    parser.add_argument("--version", action="version", version=version)
    subparsers = parser.add_subparsers(dest="command", help="Sub-commands", metavar="")

    cmds = [
        ("view", View),
    ]

    for cmd, module in cmds:
        _module = module
        _parser = subparsers.add_parser(cmd, help=_module._help)
        
        for *flags, opts in _module._cli:
            _parser.add_argument(*flags, **opts)
        
        _parser.set_defaults(func=_module.run)

    args, extras = parser.parse_known_args(argv)

    if args.command is not None:
        raise SystemExit(args.func(parser, args, extras))
    else:
        parser.print_help()

if __name__ == "__main__":
    main()