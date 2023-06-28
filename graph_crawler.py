import os
import json
import pickle
import itertools

import click

from hxxp import Requester
from authentication import EmptyToken

from universe import UniverseLookup


class DOES_NOT_EXIST:

    def __repr__(self):
        return "DOES_NOT_EXIST"


DNE = DOES_NOT_EXIST()


def slurp_json(path):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    else:
        return DNE


def dump_json(data, path):
    with open(path, "w") as f:
        return json.dump(data, f)


def slurp_pickle(path):
    if os.path.exists(path):
        with open(path, "rb") as f:
            return pickle.load(f)
    else:
        return DNE


def dump_pickle(data, path):
    with open(path, "wb") as f:
        return pickle.dump(data, f)


#
# Entry point
#


@click.command()
@click.argument("path", type=click.Path())
@click.argument("regions", nargs=-1)
def cli(regions, path):
    requester = Requester("https://esi.evetech.net/latest/", EmptyToken())
    universe = UniverseLookup(requester)

    data = slurp_pickle(path)

    if data is DNE:
        seen_systems = set([])
        seen_stargates = set([])
        edges = set([])
        print(f"Initialized new graph crawler at {path}")
    else:
        seen_systems = data["systems"]
        seen_stargates = data["seen"]
        edges = data["edges"]
        print(
            f"Loaded {len(seen_systems)} systems and "
            f"{len(seen_stargates)} known stargates from {path}"
        )

    systems = itertools.chain.from_iterable(
        universe.details("constellation", entity_id=con)["systems"]
        for con in itertools.chain.from_iterable(
            universe.details("region", name=region)["constellations"]
            for region in regions
        )
    )

    for system_id in systems:
        if system_id in seen_systems:
            continue

        stargates = universe.details(
            "system",
            entity_id=system_id,
        )["stargates"]

        for stargate_id in stargates:
            if stargate_id in seen_stargates:
                continue

            print(stargate_id)
            stargate = universe.details("stargate", entity_id=stargate_id)

            edge = (stargate["system_id"], stargate["destination"]["system_id"])
            edges.add(edge)
            seen_stargates.add(stargate_id)
            seen_stargates.add(stargate["destination"]["stargate_id"])

        seen_systems.add(system_id)
        dump_pickle(
            {
                "seen": seen_stargates,
                "systems": seen_systems,
                "edges": edges,
            },
            path,
        )

    dump_pickle(
        {
            "seen": seen_stargates,
            "systems": seen_systems,
            "edges": edges,
        },
        path,
    )


if __name__ == "__main__":
    cli()
