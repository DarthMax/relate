import os, resource, subprocess, sys, time
t = time.time()
r = subprocess.run(sys.argv[1:], stdin=open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'q.cypher')), capture_output=True, text=True)
el = time.time() - t
print(r.stdout[-6000:]); print(r.stderr[-2000:])
print(f"wall {el:.1f}s  peak RSS {resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss/1024/1024:.2f} GiB  exit {r.returncode}")
