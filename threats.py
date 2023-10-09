from dataclasses import dataclass
from pprint import pprint
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from cytoolz import get_in
from bs4 import BeautifulSoup

from formal_vector import FormalVector
from hxxp import Requester
from hxxp import DefaultHandlers
from authentication import EmptyToken

from requests.exceptions import JSONDecodeError


_json = DefaultHandlers.raise_or_return_json


r0 = Requester("https://esi.evetech.net/latest/", EmptyToken())
zk = Requester("https://zkillboard.com", EmptyToken())


from universe import UniverseLookup

universe = UniverseLookup(r0)

def kills_soup(character_name):
    character_id = universe.from_name(character_name).id
    return BeautifulSoup(zk.request("GET", f"/character/{character_id}/").text, features="html.parser")


def pad_lst_to_len(length, value=None):

    def _pad_lst_to_len(lst):
        current = len(lst)
        padding = [value]*(length - current)
        return lst + padding

    return _pad_lst_to_len
    

def killdata(character_name):
    pad = pad_lst_to_len(6, "")

    soup = kills_soup(character_name)

    danger_percents = [x.text for x in soup.find_all("div", attrs={"class": "progress-bar-danger"})]
    snuggly_percents = [
        x.text for x in soup.find_all("div", attrs={"class": "progress-bar"})
        if "progress-bar-danger" not in x.attrs.get("class", [])
    ]

    danger_numbered = [
        int(x.strip().strip("%")) if x.strip() else 0 for x in danger_percents
    ]
    snuggly_numbered = [
        int(x.strip().strip("%")) if x.strip() else 0 for x in snuggly_percents
    ]

    percents = list(zip(pad(danger_numbered), pad(snuggly_numbered)))

    dangerous = [
        danger if danger else (100 - snuggly) if snuggly else 0
        for (danger, snuggly) in percents
    ]

    match dangerous:

        case [da, _, None, *_]:
            (danger_recent, danger_all) = (0, da)

        case [da, _, dr, *_]:
            (danger_recent, danger_all) = (dr, da)

        case _:
            (danger_recent, danger_all) = (0, 0)

    

    return (danger_recent, danger_all)


class Mapper:

    def map(self, func, seq):
        for s in seq:
            yield func(s)


@contextmanager
def FlatExecutor(*args, **kwargs):
    yield Mapper()


def killdata_from_stream(stream):
    keys = [x for x in (line.strip() for line in stream) if x]
    with ThreadPoolExecutor(max_workers=6) as exe:
        values = list(exe.map(killdata, keys))

    return dict(zip(keys, values))


def ranked_threats(data):
    return sorted(((v, k) for (k, v) in data.items()), reverse=True)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--seconds", type=int, default=10)
    parser.add_argument("path")
    parsed = parser.parse_args()

    path = parsed.path
    seconds = parsed.seconds

    last_contents = tuple([])

    while True:
        with open(path, "r") as fh:
            contents = tuple(line for line in fh if line.strip())
        if contents == last_contents:
            time.sleep(seconds)
            continue
        else:
            print("\n\n\n")
            pprint(ranked_threats(killdata_from_stream(contents)))
            last_contents = contents
            time.sleep(seconds)


if __name__ == "__main__":
    main()
