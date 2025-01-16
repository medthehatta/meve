#!/usr/bin/env python

import itertools
import re
import sys


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", type=argparse.FileType("r"), nargs="?", default="-")
    parsed = parser.parse_args()

    skill_rex = re.compile(
        (
            r"^"
            r"(?P<skill>[^(]*)"
            r"("
                r"\s+\("
                r"(?P<prio>(high|med|low))"
                    r"("
                        r":(?P<max>\d)"
                    r")?"
                r"\)"
            r")?\ *$"
        ),
        re.X,
    )

    skills = []
    for line in parsed.infile:
        m = skill_rex.search(line)
        if m is None:
            print(f"Invalid line: {line.strip()}", file=sys.stderr)
        else:
            skills.append(m.groupdict())

    skills_no_none = [
        {k: v for (k, v) in skill.items() if v is not None}
        for skill in skills
    ]

    hydrated1 = [
        {"prio": "med", **entry}
        for entry in skills_no_none
    ]

    hydrated = [
        {"max": "5" if entry["prio"] == "high" else "4", **entry}
        for entry in hydrated1
    ]

    with_levels = itertools.chain.from_iterable(
        [
            (skill["prio"], i+1, skill["skill"])
            for i in range(int(skill["max"]))
        ]
        for skill in hydrated
    )

    ranks = [
        ("high", 1),
        ("high", 2),
        ("high", 3),
        ("high", 4),
        ("med", 1),
        ("med", 2),
        ("med", 3),
        ("low", 1),
        ("low", 2),
        ("med", 4),
        ("high", 5),
        ("low", 3),
        ("low", 4),
        ("med", 5),
        ("low", 5),
    ]

    skips = [
        ("med", 5),
        ("low", 4),
        ("low", 5),
    ]

    ordered = sorted(with_levels, key=lambda x: ranks.index((x[0], x[1])))

    for (prio, lvl, skill) in ordered:
        if (prio, lvl) in skips:
            print(f"Skipping {skill.strip()} {lvl}", file=sys.stderr)
        else:
            print(f"{skill.strip()} {lvl}")


if __name__ == "__main__":
    main()
