import shutil
from pprint import pprint
import pydoc
pydoc.pager = pydoc.plainpager

from authentication import EmptyToken
from hxxp import Requester
from universe import EntityFactory
from universe import ItemFactory
from universe import UniverseLookup
from universe import UserAssets

from watcher import eve_token
from watcher import TrackedMap


r0 = Requester("https://esi.evetech.net/latest/", EmptyToken())
r = Requester("https://esi.evetech.net/latest/", eve_token("token.pkl"))
universe = UniverseLookup(r0)
items = ItemFactory(r0, "types.json")
ua = UserAssets(r, "Mola Pavonis")
entity = EntityFactory(items, universe)
#db = WatcherDB("watcher.db")


shutil.rmtree("test.tm", ignore_errors=True)
tm = TrackedMap("test.tm", value_type=int)


tm.record({"foo": 1})
tm.record({"foo": 2})
tm.record({"foo": 3})
print(tm.timeseries_for_key("foo"))
print(tm.timeseries_for_key("bar"))
tm.record({"bar": 9})
tm.record({"bar": 10})
tm.record({"foo": 4})
print(tm.timeseries_for_key("foo"))
print(tm.timeseries_for_key("bar"))
tm.record({"quux": 111})
print(tm.timeseries_for_key("foo"))
print(tm.timeseries_for_key("bar"))
