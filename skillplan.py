#!/usr/bin/env python

import itertools

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", type=argparse.FileType("r"), nargs="?", default="-")
    parsed = parser.parse_args()

    skills = list(line.strip() for line in parsed.infile if line.strip())

    with_levels = itertools.chain(
        itertools.chain.from_iterable(
            [f"{skill} {i}" for i in [1, 2, 3]]
            for skill in skills
        ),
        itertools.chain.from_iterable(
            [f"{skill} {i}" for i in [4]]
            for skill in skills
        ),
        itertools.chain.from_iterable(
            [f"{skill} {i}" for i in [5]]
            for skill in skills
        ),
    )

    for entry in with_levels:
        print(entry)


if __name__ == "__main__":
    main()
