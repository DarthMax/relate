import csv, json, sys
D = sys.argv[1]
def rows(f):
    with open(f"{D}/{f}_0_0.csv") as fh:
        r = csv.DictReader(fh, delimiter='|'); yield from r
def by_name(f, name):
    return sorted(int(r['id']) for r in rows(f) if r['name'] == name)
def person(first, last):
    return sorted(int(r['id']) for r in rows("dynamic/person") if r['firstName'] == first and r['lastName'] == last)
a = {
  "tag_jamaica": by_name("static/tag", "Jamaica"),
  "tag_cate": by_name("static/tag", "Cate_Blanchett"),
  "tagclass_athlete": by_name("static/tagclass", "Athlete"),
  "tagclass_royalty": by_name("static/tagclass", "BritishRoyalty"),
  "place_tallinn": by_name("static/place", "Tallinn"),
  "place_manila": by_name("static/place", "Manila"),
  "org_linxair": by_name("static/organisation", "Linxair"),
  "person_rafael_alonso": person("Rafael", "Alonso"),
  "person_alfredo_gomez": person("Alfredo", "Gomez"),
}
print(json.dumps(a))
