from collections import Counter
import json
from math import sqrt
from pprint import pprint
import sys
import time

import diskcache
from requests import HTTPError
import xmltodict

from api_access import requester
from api_access import authed_requester

from universe import UniverseLookup
from universe import ItemFactory
from universe import EntityFactory


r0 = requester
r = authed_requester


universe = UniverseLookup(r0)
items = ItemFactory(r0, "types.json")
get_entity = EntityFactory(items, universe)


SKILL_TIME_CONSTANT_ATTR = 275


def populate_dogma_cache(
    requester,
    sleep_interval=100,
    sleep_seconds=10,
):
    cache = diskcache.Cache("eve_dogma_attributes")

    all_attrs = requester.request("GET", "/dogma/attributes")
    all_attrs.raise_for_status()

    known = 0
    found = 0
    errored = 0

    expected = len(all_attrs.json())

    for (i, attr) in enumerate(all_attrs.json(), start=1):
        if ("id", attr) in cache:
            print(f"KNOWN ({i}/{expected}) {attr} ({cache.get(('id', attr))['name']})")
            known += 1

        else:
            res = requester.request("GET", f"/dogma/attributes/{attr}")
            if res.ok:
                cache.set(("id", attr), res.json())
                print(f"FOUND ({i}/{expected}) {attr} ({cache.get(('id', attr))['name']})")
                found += 1
            else:
                print(f"ERROR ({i}/{expected}) {attr} (??) {res.status_code} {res.reason}")
                errored += 1

        if (found + 1) % sleep_interval == 0:
            print(f"SLEEP {sleep_seconds} ...")
            time.sleep(sleep_seconds)

    print(f"DONE {expected=} {known=} {found=} {errored=}")


def components_from_fits_xml(xml_contents):
    data = xmltodict.parse(xml_contents)

    fits = []

    for fit in data.get("fittings", {}).get("fitting", []):
        fits.append({
            "name": fit["@name"],
            "ship": fit["shipType"]["@value"],
            "cargo": [
                x["@type"] for x in fit["hardware"] if x["@slot"] == "cargo"
            ],
            "modules": [
                x["@type"] for x in fit["hardware"] if x["@slot"] != "cargo"
            ],
        })

    return fits


def _required_skills_from_details(details):
    dogma_attrs = {
        x["attribute_id"]: x["value"]
        for x in details.get("dogma_attributes", [])
    }

    required_skills_levels_attr_ids = [
        (182, 277),
        (183, 278),
        (184, 279),
        (1285, 1286),
        (1289, 1287),
        (1290, 1288),
    ]

    skills_required_by_id = {
        int(dogma_attrs[skill]): int(dogma_attrs[skill_level])
        for (skill, skill_level)
        in required_skills_levels_attr_ids
        if skill in dogma_attrs
    }

    return skills_required_by_id


def _required_sp(multiplier, level):
    return 250 * multiplier * sqrt(32**(level-1))


def _recursively_expand_skills(required):
    all_required = set([])

    for (skill, level) in required.items():
        skill_detail = universe.details("type", entity_id=skill)

        sp_multiplier = next(
            attr["value"] for attr in skill_detail["dogma_attributes"]
            if attr["attribute_id"] == SKILL_TIME_CONSTANT_ATTR
        )
        sp = _required_sp(sp_multiplier, level)

        all_required.add((skill, level, sp))

        subskills = _required_skills_from_details(skill_detail)

        for entry in _recursively_expand_skills(subskills):
            all_required.add(entry)

    return all_required


def skills_from_component(entity=None, name=None, entity_id=None):
    component_details = universe.details(
        "type",
        entity=entity,
        name=name,
        entity_id=entity_id,
    )

    skills_required_by_id = _required_skills_from_details(component_details)

    all_skills_by_id = _recursively_expand_skills(skills_required_by_id)

    return [
        {
            "id": skill,
            "name": get_entity.from_id(skill).name,
            "level": level,
            "sp": int(round(sp)),
        }
        for (skill, level, sp) in all_skills_by_id
    ]


def _compare_character_skills(char_skills, from_component):
    requirements = []
    total_missing_sp = 0

    char_sp = {
        s["skill_id"]: s["skillpoints_in_skill"]
        for s in char_skills
    }
    for required in from_component:
        required_sp = required["sp"]
        found_sp = char_sp.get(required["id"], 0)
        diff = max(required_sp - found_sp, 0)
        requirements.append(
            {
                "skill": f"{required['name']} {required['level']}",
                "required_sp": required_sp,
                "found_sp": found_sp,
                "missing": diff,
            }
        )
        total_missing_sp += diff

    return {
        "total_missing_sp": total_missing_sp,
        "all_skills": requirements,
        "missing_skills": [
            {
                "skill": skill["skill"],
                "missing": skill["missing"],
            }
            for skill in requirements if skill["missing"] > 0
        ],
    }


def _character_skills(authed_requester, name):
    character_id = get_entity.from_name(name).id
    res = authed_requester.request("GET", f"/characters/{character_id}/skills")
    res.raise_for_status()
    found = res.json()["skills"]
    return found


def compare_character_skills(authed_requester, name, from_component):
    found = _character_skills(authed_requester, name)
    return _compare_character_skills(found, from_component)


def skills_from_components(
    entities=None,
    entity_names=None,
    entity_ids=None,
):
    skills_each = []

    all_entities = (
        (entities or [])
        + (get_entity.from_name_seq(entity_names) if entity_names else [])
        + (get_entity.from_id_seq(entity_ids) if entity_ids else [])
    )

    skills_each = [
        (entity.name, skills_from_component(entity))
        for entity in all_entities
    ]

    total_skills = {}

    for (entity_name, skills) in skills_each:
        for skill in skills:
            skid = skill["id"]
            if skid in total_skills:
                if skill["level"] <= total_skills[skid]["level"]:
                    continue
            # Write this skill entry if it doesn't exist or if this entry
            # outlevels the existing one
            total_skills[skid] = {**skill, "for": entity_name}
        
    return list(total_skills.values())


def skills_from_fit(fit):
    all_entities = [fit["ship"]] + fit["modules"] + fit["cargo"]
    return skills_from_components(entity_names=all_entities)


def skills_from_fits_xml(xml_contents, cache_name=None):
    if cache_name:
        cache = diskcache.Cache(cache_name)
    else:
        cache = {}

    fits = components_from_fits_xml(xml_contents)

    for fit in fits:

        if ("name", fit["name"]) not in cache:
            skills = skills_from_fit(fit)
            cache[("name", fit["name"])] = skills

        skills = cache.get(("name", fit["name"]))

        yield {
            "fit": fit["name"],
            "skills": skills,
        }


def _trace(msg):
    print(msg, file=sys.stderr, flush=True)


def json_dump_skills_from_fits_xml(
    xml_contents,
    cache_name="doctrine_fits",
    sleep_interval=10,
    sleep_seconds=10,
    suppress_known=True,
):
    cache = diskcache.Cache(cache_name)

    fits = components_from_fits_xml(xml_contents)

    num_fits = len(fits)

    fetches_attempted = 0

    for (n, fit) in enumerate(fits, start=1):
        if ("name", fit["name"]) in cache:
            skills = cache.get(("name", fit["name"]))
            if not suppress_known:
                print(json.dumps({"fit": fit["name"], "skills": skills}), flush=True)
                _trace(f"KNOWN ({n}/{num_fits}) {fit['name']}")
            continue

        try:
            skills = skills_from_fit(fit)
            cache.set(("name", fit["name"]), skills)
            _trace(f"FETCHED ({n}/{num_fits}) {fit['name']}")
            print(json.dumps({"fit": fit["name"], "skills": skills}), flush=True)

        except HTTPError as err:
            _trace(f"ERROR ({n}/{num_fits}) {fit['name']} {err.status_code} {err.reason}")
            _trace(f"SLEEP ({n}/{num_fits}) for {sleep_seconds}s HTTP error throttle")
            time.sleep(sleep_seconds)

        fetches_attempted += 1

        if fetches_attempted % sleep_interval == 0:
            _trace(f"SLEEP ({n}/{num_fits}) for {sleep_seconds}s interval reached")
            time.sleep(sleep_seconds)


def compare_fits_from_cache(authed_requester, character_name, cache_name):

    char_skills = _character_skills(authed_requester, character_name)

    compare_by_fit = {}

    cache = diskcache.Cache(cache_name)

    for key in cache.iterkeys():
        (_, fit_name) = key
        from_component = cache.get(key)
        compare_by_fit[fit_name] = \
            _compare_character_skills(char_skills, from_component)

    result = [
        {
            "name": fit_name,
            "missing_skills": reqs["missing_skills"],
            "missing_sp": reqs["total_missing_sp"],
        }
        for (fit_name, reqs) in compare_by_fit.items()
    ]

    return result


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skills",
        action="store_true",
        help="Print skills required for the provided fits XML",
    )
    parser.add_argument(
        "--missing",
        action="store_true",
        help=(
            "Print skills required for using the fits in the XML which are "
            "missing from the given character"
        ),
    )
    parser.add_argument(
        "--histogram",
        action="store_true",
        help=(
            "Print the skills required by fits in the XML, sorted by "
            "frequency of occurrence"
        ),
    )
    parser.add_argument(
        "--missing-histogram",
        action="store_true",
        help=(
            "Print skills required for using the fits in the XML which are "
            "missing from the given character sorted by frequency of "
            "occurrence"
        ),
    )
    parser.add_argument(
        "--list-fits",
        "--list",
        action="store_true",
        help="List the fit names in the XML",
    )
    parser.add_argument(
        "--login",
        action="store_true",
        help="Just perform an OIDC login and save the token",
    )
    parser.add_argument("--purge", action="store_true")
    parser.add_argument("-c", "--character-name", "--character")
    parser.add_argument("-C", "--cache-name", "--cache", default="doctrine_fits")
    parser.add_argument("-i", "--sleep-interval", "--interval", type=int)
    parser.add_argument("-d", "--sleep-duration", "--duration", type=int)
    parser.add_argument("--always-trace", "--always", action="store_true")
    parser.add_argument("-s", "--sort", action="store_true")
    parser.add_argument("-r", "--reverse", action="store_true")
    parser.add_argument("xml_file", type=argparse.FileType("r"), nargs="?")
    parsed = parser.parse_args()

    xml_file = parsed.xml_file
    character_name = parsed.character_name
    cache_name = parsed.cache_name
    sleep_interval = parsed.sleep_interval
    sleep_duration = parsed.sleep_duration
    suppress_known = not parsed.always_trace
    do_sort = parsed.sort
    sort_reverse = parsed.reverse

    if xml_file:
        xml_contents = xml_file.read()
        json_dump_skills_from_fits_xml(
            xml_contents,
            cache_name,
            sleep_interval=sleep_interval,
            sleep_seconds=sleep_duration,
            suppress_known=suppress_known,
        )

    else:
        xml_contents = ""

    if parsed.skills:
        skills = skills_from_fits_xml(xml_contents, cache_name=cache_name)
        for s in skills:
            print(json.dumps(s))

    elif parsed.missing:
        if not character_name:
            raise ValueError("Must provide --character-name if --missing provided")

        comparison = compare_fits_from_cache(r, character_name, cache_name)

        if do_sort:
            itr = sorted(
                comparison,
                key=lambda x: x.get("missing_sp"),
                reverse=sort_reverse,
            )
        else:
            itr = comparison

        for c in itr:
            print(json.dumps(c))

    elif parsed.histogram:
        fits = skills_from_fits_xml(xml_contents, cache_name=cache_name)

        histogram = Counter()
        for fit_entry in fits:
            for skill in fit_entry.get("skills", []):
                histogram.update([f"{skill['name']} {skill['level']}"])

        if sort_reverse:
            listing = reversed(histogram.most_common())
        else:
            listing = histogram.most_common()

        for (skill, count) in listing:
            print(f"{count} {skill}")

    elif parsed.missing_histogram:
        if not character_name:
            raise ValueError("Must provide --character-name if --missing provided")

        comparison = compare_fits_from_cache(r, character_name, cache_name)

        histogram = Counter()
        for fit_entry in comparison:
            for skill_entry in fit_entry.get("missing_skills", []):
                histogram.update([skill_entry["skill"]])

        if sort_reverse:
            listing = reversed(histogram.most_common())
        else:
            listing = histogram.most_common()

        for (skill, count) in listing:
            print(f"{count} {skill}")

    elif parsed.list_fits:
        fits = components_from_fits_xml(xml_contents)
        for fit in fits:
            print(f"{fit['ship']} | {fit['name']}")

    elif parsed.login:
        r.token.get()

    else:
        print("No output requested")


if __name__ == "__main__":
    main()
