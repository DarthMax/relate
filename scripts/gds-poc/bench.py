import json, os, re, statistics, subprocess, sys, time, resource
HERE = os.path.dirname(os.path.abspath(__file__))
A = json.load(open(f'{HERE}/anchors.json'))
Q = {
 "q03": "MATCH (post:Post {K: 962077547172})-[:postHasCreator]->(person:Person), (person)-[:studyAt]->(org:Organisation) RETURN person.K AS p, org.K AS o ORDER BY p, o",
 "q04": f"MATCH (p:Person)<-[:commentHasCreator]-(c:Comment) WHERE p.K IN {A['person_alfredo_gomez']} AND c.length > 100 RETURN count(c) AS n",
 "q07": f"MATCH (p:Person {{K: 1786706544494}})<-[:commentHasCreator]-(c:Comment)-[:replyOfPost]->(post:Post)-[:postHasTag]->(t:Tag), (c)-[:commentIsLocatedIn]->(place:Place) WHERE t.K IN {A['tag_jamaica']} RETURN DISTINCT place.K AS place ORDER BY place",
 "q09": f"MATCH (p:Person)-[:knows]->(p2:Person)-[:studyAt]->(o:Organisation)-[:organisationIsLocatedIn]->(l:Place) WHERE l.K IN {A['place_tallinn']} RETURN count(*) AS n",
 "q10": f"MATCH (c:Comment)-[:replyOfPost]->(post:Post)-[:postHasTag]->(t:Tag), (c)-[:commentHasCreator]->(p:Person) WHERE t.K IN {A['tag_cate']} RETURN count(DISTINCT p.K) AS n",
 "q13": f"MATCH (p:Person)<-[:commentHasCreator]-(c:Comment)<-[:likeComment]-(p2:Person) WHERE p.K IN {A['person_rafael_alonso']} RETURN count(DISTINCT p2.K) AS n",
 "q14": f"MATCH (f:Forum)-[:forumHasTag]->(:Tag)-[:hasType]->(tc:Tagclass) WHERE tc.K IN {A['tagclass_athlete']} RETURN count(DISTINCT f.K) AS n",
 "q20n": "MATCH (c:Comment) WHERE c.length > 1000 RETURN count(c) AS n",
 "q22": f"MATCH (p:Person)-[:workAt]->(o:Organisation), (c:Comment)-[:replyOfPost]->(post:Post), (c)-[:commentHasCreator]->(p) WHERE o.K IN {A['org_linxair']} RETURN count(DISTINCT c.K) AS n",
 "q28": f"MATCH (t:Tag)-[:hasType]->(tc:Tagclass), (p:Person)-[:hasInterest]->(t), (p)-[:personIsLocatedIn]->(l:Place) WHERE tc.K IN {A['tagclass_royalty']} AND l.K IN {A['place_manila']} RETURN count(DISTINCT p.K) AS n",
 "q30": "MATCH (c:Comment)-[:commentHasCreator]->(creator:Person), (c)-[:replyOfPost]->(post:Post)-[:postHasCreator]->(creator) RETURN count(DISTINCT c.K) AS n",
 "knows2": "MATCH (:Person)-[:knows]->(:Person)-[:knows]->(fof:Person) RETURN count(*) AS n",
}
REPS = 5
def run(store, key):
    names, stmts = [], []
    for name, q in Q.items():
        for _ in range(REPS):
            names.append(name); stmts.append(q.replace(".K", "." + key).replace("{K:", "{" + key + ":") + ";")
    t = time.time()
    r = subprocess.run([os.environ.get("RELATE_BIN", "target/release/relate"), "query", "--read-only", "-d", store],
                       input="\n".join(stmts) + "\n", capture_output=True, text=True)
    wall = time.time() - t
    if r.returncode:
        sys.exit(r.stderr[-3000:])
    lines = r.stdout.splitlines()
    out, i, cur = [], 0, []
    while i < len(lines):
        l = lines[i]
        m = re.match(r"rows\[(\d+)\]", l)
        if m:
            n = int(m.group(1)); cur = [x.strip() for x in lines[i + 1:i + 1 + n]]; i += 1 + n; continue
        if l.startswith("timings:"):
            out.append((cur, json.loads(lines[i + 1])["elapsed_ms"])); cur = []; i += 2; continue
        i += 1
    assert len(out) == len(names), (len(out), len(names), r.stdout[-2000:])
    res = {}
    for name, (rows, ms) in zip(names, out):
        res.setdefault(name, {"rows": rows, "ms": []})["ms"].append(ms)
    return res, wall, resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1048576
store, key, label = sys.argv[1], sys.argv[2], sys.argv[3]
res, wall, rss = run(store, key)
json.dump({"label": label, "wall": wall, "res": res}, open(f"target/bench/{label}.json", "w"))
print(f"{label}: session wall {wall:.1f}s, peak RSS (cumulative children) {rss:.2f} GiB")
for name, v in res.items():
    print(f"  {name:7} median {statistics.median(v['ms']):9.2f} ms  rows {v['rows']}")
