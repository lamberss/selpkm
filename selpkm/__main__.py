import argparse
import pathlib
from typing import Sequence, Union

def get_config_dir():
    return pathlib.Path(f'~/.{__package__}').expanduser()

def get_database_path():
    return get_config_dir() / f'{__package__}.db'

def main():
    print(f'Hello, world (from main)!')
    print(f'database_path = {get_database_path()}')
    args = parse_commands()
    print(args)

def parse_commands(args: Union[Sequence[str], None] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog=__package__)

    parser.add_argument('--capture', '-c', help='quick capture an idea')

    return parser.parse_args(args=args)


if __name__ == '__main__':
    main()