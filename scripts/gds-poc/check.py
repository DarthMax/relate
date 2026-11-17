import csv, sys
from datetime import datetime
D = sys.argv[1]
def rows(f):
    with open(f"{D}/{f}_0_0.csv") as fh:
        r = csv.reader(fh, delimiter='|'); next(r); yield from r
knows = [(int(a), int(b), c) for a, b, c in rows("dynamic/person_knows_person")]
adj = {}
for a, b, _ in knows:
    adj.setdefault(a, []).append(b); adj.setdefault(b, []).append(a)
print("friends(933)", len(adj[933]))
fof = {y for x in adj[933] for y in adj.get(x, []) if y != 933}
print("fof(933)", len(fof))
ms = lambda s: int(datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z").timestamp() * 1000)
posts = [int(p) for p, c in rows("dynamic/post_hasCreator_person") if int(c) == 933]
pc = {int(r[0]): r[2] for r in rows("dynamic/post") if int(r[0]) in set(posts)}
print("posts(933)", len(posts), "latest", max(ms(v) for v in pc.values()))
print("knows first/last", min(ms(c) for *_, c in knows), max(ms(c) for *_, c in knows))
city = [int(p) for a, p in rows("dynamic/person_isLocatedIn_place") if int(a) == 933][0]
print("city", city, "country", [int(b) for a, b in rows("static/place_isPartOf_place") if int(a) == city])
