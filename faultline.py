#!/usr/bin/env python3
"""
faultline: deterministic fault-injection lab for consensus. One file, zero dependencies.

  Sim         discrete-event simulator, virtual time, one seed = one exact universe
  Node        Raft: election, replication, crash recovery, client sessions (exactly-once), linearizable KV
  nemesis     seeded schedule: partitions, leader isolation, crashes, rival campaigns, voter power-cycles
  invariants  one vote per term, election safety, state-machine safety, checked online
  check_key   Wing-Gong linearizability checker with memoisation (per-key, compositional)
  shrink      delta-debugging: failing run -> minimal fault schedule
  MUTANTS     four classic Raft bugs injected on purpose; the lab must catch every one

Usage:
  python faultline.py selftest
  python faultline.py hunt [seeds]          sweep correct Raft and every mutant
  python faultline.py shrink <mutant> [seed]
  python faultline.py run <seed> [mutant]

Research and education tool: simulated nodes only, never touches a real network.
"""
from __future__ import annotations
import hashlib, heapq, sys

INF = 1 << 60
TAIL, TIMEOUT = 2500, 600
MUTANTS = {
    "vote_stale":      "grants votes to candidates with a stale log",
    "commit_minority": "commits on 2 replicas instead of a majority",
    "stale_reads":     "leader answers reads from local state, no log round-trip",
    "forget_vote":     "voted_for is not persisted across restarts",
}


class Rng:
    M = (1 << 64) - 1

    def __init__(s, seed): s.x = seed & s.M

    def next(s):
        s.x = (s.x + 0x9E3779B97F4A7C15) & s.M
        z = s.x
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & s.M
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & s.M
        return z ^ (z >> 31)

    def below(s, n): return s.next() % n
    def between(s, a, b): return a + s.next() % (b - a + 1)


# ------------------------------------------------------------------ Raft node
class Node:
    def __init__(s, sim, i):
        s.sim, s.i, s.alive = sim, i, True
        s.term, s.voted, s.log = 0, None, []          # persistent
        s.te = s.th = 0
        s.reset()

    def reset(s):                                      # volatile
        s.role, s.leader, s.commit, s.applied = "F", None, 0, 0
        s.kv, s.sess, s.votes, s.nxt, s.match, s.pend = {}, {}, set(), {}, {}, {}

    def peers(s): return [p for p in range(s.sim.n) if p != s.i]
    def send(s, p, m): s.sim.send(s.i, p, m)

    def reset_timer(s):
        s.te += 1
        s.sim.push(s.sim.t + 150 + s.sim.rng.below(60), "elect", (s.i, s.te))

    def crash(s):
        s.alive = False; s.te += 1; s.th += 1

    def restart(s):
        s.alive = True; s.reset()
        if s.sim.mutant == "forget_vote": s.voted = None
        s.reset_timer()

    def step_down(s, term):
        if term > s.term: s.term, s.voted = term, None
        s.role = "F"; s.pend.clear(); s.th += 1; s.reset_timer()

    def election(s):
        s.term += 1; s.role = "C"; s.voted = s.i; s.votes = {s.i}; s.reset_timer()
        li = len(s.log); lt = s.log[-1][0] if s.log else 0
        for p in s.peers(): s.send(p, ("RV", s.term, s.i, li, lt))

    def become_leader(s):
        s.role = "L"; s.th += 1; s.leader = s.i
        if s.sim.leaders.setdefault(s.term, s.i) != s.i:
            s.sim.violate("election-safety", f"two leaders in term {s.term}")
        s.log.append((s.term, ("noop", None, None, -1, 0)))
        for p in s.peers(): s.nxt[p], s.match[p] = len(s.log), 0
        s.heartbeat()

    def heartbeat(s):
        if s.role != "L": return
        for p in s.peers(): s.send_append(p)
        s.sim.push(s.sim.t + 50, "hb", (s.i, s.th))

    def send_append(s, p):
        ni = min(s.nxt[p], len(s.log) + 1); prev = ni - 1
        pt = s.log[prev - 1][0] if prev > 0 else 0
        s.send(p, ("AE", s.term, s.i, prev, pt, tuple(s.log[ni - 1:ni + 15]), s.commit))

    def advance_commit(s):
        need = 2 if s.sim.mutant == "commit_minority" else s.sim.n // 2 + 1
        for N in range(len(s.log), s.commit, -1):
            if s.log[N - 1][0] != s.term: break
            if 1 + sum(1 for p in s.peers() if s.match[p] >= N) >= need:
                s.commit = N; s.apply(); break

    def apply(s):
        while s.applied < s.commit:
            s.applied += 1; e = s.log[s.applied - 1]
            s.sim.check_apply(s.applied, e)
            kind, key, val, cid, rid = e[1]
            if kind != "noop" and s.sess.get(cid, (0, None))[0] >= rid:
                res = s.sess[cid][1]                       # duplicate delivery: exactly-once via session table
            else:
                if kind == "put": s.kv[key] = val; res = "ok"
                elif kind == "get": res = s.kv.get(key)
                else: res = None
                if kind != "noop": s.sess[cid] = (rid, res)
            w = s.pend.pop(s.applied, None)
            if w and s.role == "L":
                s.send(-1 - w[0], ("RESP", w[1], "ok", res, s.i))

    def on_msg(s, src, m):
        k = m[0]
        if k == "RV":
            _, term, cand, li, lt = m
            if term > s.term: s.step_down(term)
            mlt = s.log[-1][0] if s.log else 0
            utd = lt > mlt or (lt == mlt and li >= len(s.log))
            if s.sim.mutant == "vote_stale": utd = True
            grant = term == s.term and s.voted in (None, cand) and utd
            if grant:
                s.voted = cand; s.reset_timer()
                if s.sim.ballots.setdefault((s.i, s.term), cand) != cand:
                    s.sim.violate("double-vote", f"node {s.i} voted twice in term {s.term}")
            s.send(cand, ("RVR", s.term, grant, s.i))
        elif k == "RVR":
            _, term, granted, frm = m
            if term > s.term: s.step_down(term); return
            if s.role == "C" and term == s.term and granted:
                s.votes.add(frm)
                if len(s.votes) >= s.sim.n // 2 + 1: s.become_leader()
        elif k == "AE":
            _, term, ld, prev, pt, ents, lc = m
            if term < s.term: s.send(ld, ("AER", s.term, False, s.i, len(s.log))); return
            if term > s.term: s.term, s.voted = term, None
            if s.role != "F": s.role = "F"; s.pend.clear(); s.th += 1
            s.leader = ld; s.reset_timer()
            if prev > len(s.log) or (prev > 0 and s.log[prev - 1][0] != pt):
                s.send(ld, ("AER", s.term, False, s.i, min(len(s.log), prev - 1))); return
            for j, e in enumerate(ents):
                idx = prev + 1 + j
                if idx <= len(s.log):
                    if s.log[idx - 1][0] != e[0]: del s.log[idx - 1:]; s.log.append(e)
                else: s.log.append(e)
            if lc > s.commit: s.commit = min(lc, prev + len(ents)); s.apply()
            s.send(ld, ("AER", s.term, True, s.i, prev + len(ents)))
        elif k == "AER":
            _, term, ok, frm, mi = m
            if term > s.term: s.step_down(term); return
            if s.role == "L" and term == s.term:
                if ok:
                    s.match[frm] = max(s.match[frm], mi); s.nxt[frm] = s.match[frm] + 1; s.advance_commit()
                else:
                    s.nxt[frm] = max(1, min(s.nxt[frm] - 1, mi + 1)); s.send_append(frm)
        elif k == "REQ":
            kind, key, val, cid, rid = cmd = m[1]
            if s.role != "L": s.send(-1 - cid, ("RESP", rid, "redirect", s.leader, s.i)); return
            if kind == "get" and s.sim.mutant == "stale_reads":
                s.send(-1 - cid, ("RESP", rid, "ok", s.kv.get(key), s.i)); return
            s.log.append((s.term, cmd)); s.pend[len(s.log)] = (cid, rid)
            for p in s.peers(): s.send_append(p)


# ------------------------------------------------------------------ clients
class Client:
    def __init__(s, sim, cid):
        s.sim, s.cid, s.rid, s.op, s.hint = sim, cid, 0, None, None

    def start(s):
        sim = s.sim
        if sim.t >= sim.horizon: return
        s.rid += 1
        kind = "put" if sim.rng.below(100) < 50 else "get"
        key = sim.rng.below(sim.keys)
        sim.vc += 1
        val = sim.vc if kind == "put" else None
        s.op = [s.cid, s.rid, kind, key, val, sim.t, None, None, None]
        sim.hist.append(s.op)
        tgt = s.hint if (s.hint is not None and sim.rng.below(100) < 70) else sim.rng.below(sim.n)
        sim.send(-1 - s.cid, tgt, ("REQ", (kind, key, val, s.cid, s.rid)))
        sim.push(sim.t + TIMEOUT, "cto", (s.cid, s.rid))

    def live(s, rid): return s.op is not None and s.op[1] == rid and s.op[7] is None

    def on_resp(s, m):
        _, rid, status, payload, frm = m
        if not s.live(rid): return
        op = s.op; op[6] = s.sim.t
        if status == "ok": op[7], op[8], s.hint = "ok", payload, frm
        else: op[7], s.hint = "fail", payload
        s.sim.push(s.sim.t + s.sim.rng.between(5, 50), "cnext", (s.cid,))

    def timeout(s, rid):
        if not s.live(rid): return
        s.op[7], s.op[6], s.hint = "info", INF, None
        s.sim.push(s.sim.t + 10, "cnext", (s.cid,))


# ------------------------------------------------------------------ nemesis
def gen_schedule(seed, n, horizon):
    r, out, t = Rng(seed * 0x9E3779B1 + 12345), [], 300
    while t < horizon - 800:
        x = r.below(100)
        if x < 28:
            pick = set()
            while len(pick) < 1 + r.below(n // 2): pick.add(r.below(n))
            out.append((t, "part", tuple(sorted(pick))))
        elif x < 40: out.append((t, "isolate_leader", None))
        elif x < 50: out.append((t, "heal", None))
        elif x < 62: out.append((t, "crash", r.below(n)))
        elif x < 72: out.append((t, "restart", r.below(n)))
        elif x < 80: out.append((t, "bounce", r.below(n)))
        else:                                              # election storm: rival campaigns + voter power-cycles
            a = r.below(n); b = (a + 1 + r.below(n - 1)) % n
            out += [(t, "campaign", a), (t + r.between(0, 12), "campaign", b),
                    (t + r.between(8, 30), "bounce_voter", None), (t + r.between(20, 50), "bounce_voter", None)]
        t += r.between(150, 600)
    return out


# ------------------------------------------------------------------ simulator
class Sim:
    def __init__(s, seed, mutant=None, horizon=6000, clients=4, schedule=None, n=5, keys=3):
        s.seed, s.mutant, s.horizon, s.n, s.keys = seed, mutant, horizon, n, keys
        s.rng, s.q, s.seq, s.t, s.vc = Rng(seed), [], 0, 0, 0
        s.nodes = [Node(s, i) for i in range(n)]
        s.clients = [Client(s, c) for c in range(clients)]
        s.group, s.drop_pm = [0] * n, 10
        s.leaders, s.applied_log, s.viol, s.hist, s.ballots = {}, {}, [], [], {}
        s.trace = hashlib.blake2b(digest_size=16)
        s.schedule = gen_schedule(seed, n, horizon) if schedule is None else list(schedule)

    def push(s, t, kind, args):
        s.seq += 1
        heapq.heappush(s.q, (t, s.seq, kind, args))

    def send(s, src, dst, msg):
        if s.rng.below(1000) < s.drop_pm: return
        d = 3 + s.rng.below(18)
        if s.rng.below(100) < 3: d += 100 + s.rng.below(100)
        s.push(s.t + d, "msg", (src, dst, msg))
        if src >= 0 and dst >= 0 and s.rng.below(1000) < 10:      # duplicates: node-to-node only
            s.push(s.t + d + s.rng.below(30), "msg", (src, dst, msg))

    def violate(s, kind, detail):
        if kind not in [v[0] for v in s.viol]: s.viol.append((kind, detail))

    def check_apply(s, idx, e):
        p = s.applied_log.setdefault(idx, e)
        if p != e: s.violate("state-machine-safety", f"index {idx} applied as two different entries")

    def down(s): return sum(1 for nd in s.nodes if not nd.alive)

    def fault(s, a, arg):
        n = s.n
        if a == "part": s.group = [1 if i in arg else 0 for i in range(n)]
        elif a == "isolate_leader":
            ls = [nd for nd in s.nodes if nd.alive and nd.role == "L"]
            if ls:
                l = max(ls, key=lambda nd: nd.term); s.group = [2 if i == l.i else 0 for i in range(n)]
        elif a == "heal": s.group = [0] * n
        elif a == "crash":
            if s.nodes[arg].alive and s.down() < n // 2: s.nodes[arg].crash()
        elif a == "restart":
            if not s.nodes[arg].alive: s.nodes[arg].restart()
        elif a == "bounce":
            if s.nodes[arg].alive and s.down() < n // 2:
                s.nodes[arg].crash(); s.push(s.t + 5 + s.rng.below(20), "fault", ("restart", arg))
        elif a == "campaign":          # force a node to stand for election right now
            if s.nodes[arg].alive and s.nodes[arg].role != "L": s.nodes[arg].election()
        elif a == "bounce_voter":      # targeted: power-cycle a follower that has just cast a vote
            vs = [nd for nd in s.nodes if nd.alive and nd.role == "F" and nd.voted is not None]
            if vs and s.down() < n // 2:
                v = max(vs, key=lambda nd: (nd.term, nd.i)); v.crash()
                s.push(s.t + 5 + s.rng.below(20), "fault", ("restart", v.i))
        elif a == "quiesce":
            s.group, s.drop_pm = [0] * n, 0
            for nd in s.nodes:
                if not nd.alive: nd.restart()

    def run(s):
        for t, a, arg in s.schedule: s.push(t, "fault", (a, arg))
        s.push(s.horizon, "fault", ("quiesce", None))
        for c in s.clients: s.push(50 + 10 * c.cid, "cnext", (c.cid,))
        for nd in s.nodes: nd.reset_timer()
        end = s.horizon + TAIL
        while s.q:
            t, _, kind, a = heapq.heappop(s.q)
            if t > end: break
            s.t = t
            s.trace.update(repr((t, kind, a)).encode())
            if kind == "msg":
                src, dst, m = a
                if dst < 0: s.clients[-1 - dst].on_resp(m)
                else:
                    nd = s.nodes[dst]
                    if nd.alive and (src < 0 or s.group[src] == s.group[dst]): nd.on_msg(src, m)
            elif kind == "elect":
                nd = s.nodes[a[0]]
                if nd.alive and a[1] == nd.te and nd.role != "L": nd.election()
            elif kind == "hb":
                nd = s.nodes[a[0]]
                if nd.alive and a[1] == nd.th and nd.role == "L": nd.heartbeat()
            elif kind == "fault": s.fault(*a)
            elif kind == "cnext": s.clients[a[0]].start()
            elif kind == "cto": s.clients[a[0]].timeout(a[1])
        ok, key, ops = check_history(s.hist)
        kinds = [v[0] for v in s.viol] + (["linearizability"] if ok is False else [])
        st = lambda x: sum(1 for o in s.hist if o[7] == x)
        return dict(bad=bool(kinds), kinds=kinds, detail=[v[1] for v in s.viol], lin=ok, lin_key=key, lin_ops=ops,
                    ops=len(s.hist), ok=st("ok"), info=st("info"), fail=st("fail"),
                    committed=len(s.applied_log), terms=len(s.leaders), head=s.trace.hexdigest())


# ------------------------------------------------------------------ linearizability
class Budget(Exception): pass


def check_key(ops, budget=2_000_000):
    """ops: (inv, ret, kind, val, result); ret == INF means indeterminate. Wing-Gong + memo."""
    ops = sorted(ops, key=lambda o: (o[0], o[1])); n = len(ops)
    comp = sum(1 << i for i, o in enumerate(ops) if o[1] < INF)
    seen, steps = set(), [0]

    def dfs(mask, val):
        if mask & comp == comp: return True
        if (mask, val) in seen: return False
        seen.add((mask, val)); steps[0] += 1
        if steps[0] > budget: raise Budget
        mr = min(ops[i][1] for i in range(n) if not (mask >> i) & 1 and ops[i][1] < INF)
        for i in range(n):
            if (mask >> i) & 1: continue
            o = ops[i]
            if o[0] > mr: break
            if o[2] == "put": ok, nv = True, o[3]
            else: ok, nv = o[4] == val, val
            if ok and dfs(mask | (1 << i), nv): return True
        return False

    try: return dfs(0, None)
    except Budget: return None


def check_history(hist):
    by = {}
    for cid, rid, kind, key, val, inv, ret, st, res in hist:
        if st == "fail" or (kind == "get" and st != "ok"): continue
        by.setdefault(key, []).append((inv, ret if st == "ok" else INF, kind, val, res))
    unknown = False
    for key in sorted(by):
        r = check_key(by[key])
        if r is False: return False, key, sorted(by[key])
        unknown |= r is None
    return (None if unknown else True), None, []


# ------------------------------------------------------------------ shrinker
def shrink(seed, mutant, horizon=6000, clients=4, budget=300):
    sched, runs = list(Sim(seed, mutant, horizon, clients).schedule), [0]

    def bad(sc, hz, cl):
        runs[0] += 1
        return Sim(seed, mutant, hz, cl, sc).run()["bad"]

    assert bad(sched, horizon, clients), "seed does not fail"
    n = 2
    while len(sched) >= 2 and runs[0] < budget:
        chunk, hit = max(1, len(sched) // n), False
        for i in range(0, len(sched), chunk):
            cand = sched[:i] + sched[i + chunk:]
            if bad(cand, horizon, clients): sched, n, hit = cand, max(n - 1, 2), True; break
        if not hit:
            if chunk == 1: break
            n = min(len(sched), n * 2)
    if sched and bad([], horizon, clients): sched = []
    for cl in range(clients - 1, 0, -1):
        if bad(sched, horizon, cl): clients = cl
        else: break
    while int(horizon * 0.8) >= 1500:
        hz = int(horizon * 0.8); cand = [e for e in sched if e[0] < hz]
        if bad(cand, hz, clients): horizon, sched = hz, cand
        else: break
    r = Sim(seed, mutant, horizon, clients, sched).run()
    return dict(schedule=sched, horizon=horizon, clients=clients, runs=runs[0], result=r)


# ------------------------------------------------------------------ CLI
def sweep(mutant, seeds):
    fails, first, kinds = 0, None, {}
    for sd in range(1, seeds + 1):
        r = Sim(sd, mutant).run()
        if r["bad"]:
            fails += 1; first = first or sd
            for k in r["kinds"]: kinds[k] = kinds.get(k, 0) + 1
    return fails, first, kinds


def hunt(seeds=100):
    print(f"{seeds} seeds x 5 nodes x 4 clients x 6s of virtual time per variant\n")
    print(f"{'variant':<17}{'violations':>12}{'first seed':>12}   caught by")
    for m in (None,) + tuple(MUTANTS):
        f, first, kinds = sweep(m, seeds)
        print(f"{(m or 'raft (correct)'):<17}{f'{f}/{seeds}':>12}{(first or '-'):>12}   {', '.join(sorted(kinds)) or '-'}")


def show(sh, mutant, seed):
    r = sh["result"]
    print(f"{mutant} seed {seed}: minimal repro = {len(sh['schedule'])} fault(s), {sh['clients']} client(s), "
          f"{sh['horizon']} ms  ({sh['runs']} runs)")
    for t, a, arg in sh["schedule"]: print(f"  t={t:>5}ms  {a}{'' if arg is None else ' ' + str(arg)}")
    print("  violation:", ", ".join(r["kinds"]), "|", "; ".join(r["detail"]))
    if r["lin"] is False:
        ops = r["lin_ops"]
        k = next(k for k in range(1, len(ops) + 1) if check_key([o for o in ops if o[0] <= ops[k - 1][0]]) is False)
        pre = [o for o in ops if o[0] <= ops[k - 1][0]]
        print(f"  key {r['lin_key']} stops being linearizable at the last operation below:")
        for j, (inv, ret, kind, val, res) in enumerate(pre[-8:]):
            mark = "  <-- no legal order exists" if j == len(pre[-8:]) - 1 else ""
            print(f"    [{inv:>5},{'  ???' if ret >= INF else format(ret, '5d')}]  "
                  f"{'put(' + str(val) + ')' if kind == 'put' else 'get -> ' + str(res)}{mark}")


def selftest():
    H = lambda *o: list(o)
    assert check_key(H((0, 10, "put", 1, None), (20, 30, "get", None, 1))) is True
    assert check_key(H((0, 10, "put", 1, None), (20, 30, "put", 2, None), (40, 50, "get", None, 1))) is False
    assert check_key(H((0, 50, "put", 1, None), (10, 20, "get", None, None))) is True
    assert check_key(H((0, INF, "put", 1, None), (10, 20, "get", None, 1))) is True
    assert check_key(H((10, 20, "get", None, 1))) is False
    print("checker      OK  5 hand-built histories classified correctly")
    a, b = Sim(5).run(), Sim(5).run()
    assert a == b and a["head"] != Sim(6).run()["head"]
    print(f"determinism  OK  seed 5 -> trace {a['head'][:12]}, bit-identical on rerun")
    clean = [Sim(s).run() for s in range(1, 41)]
    assert not any(r["bad"] for r in clean), [r["kinds"] for r in clean if r["bad"]]
    print(f"raft         OK  40 chaotic seeds, 0 violations, {sum(r['ok'] for r in clean)} ops acked, "
          f"{sum(r['info'] for r in clean)} indeterminate, all linearizable")
    first = {}
    for m in MUTANTS:
        for sd in range(1, 201):
            r = Sim(sd, m).run()
            if r["bad"]: first[m] = sd; break
        assert m in first, f"mutant {m} escaped 200 seeds"
        print(f"mutant       OK  {m:<16} caught at seed {first[m]}")
    sh = shrink(first["stale_reads"], "stale_reads")
    full = len(Sim(first["stale_reads"], "stale_reads").schedule)
    assert len(sh["schedule"]) < full and sh["result"]["bad"]
    print(f"shrinker     OK  stale_reads: {full} faults -> {len(sh['schedule'])}, "
          f"{6000} ms -> {sh['horizon']} ms, still fails")
    print("selftest: all OK")


if __name__ == "__main__":
    a = sys.argv[1:]
    if not a: print(__doc__)
    elif a[0] == "selftest": selftest()
    elif a[0] == "hunt": hunt(int(a[1]) if len(a) > 1 else 100)
    elif a[0] == "shrink":
        m = a[1] if len(a) > 1 else "stale_reads"
        sd = int(a[2]) if len(a) > 2 else next(x for x in range(1, 400) if Sim(x, m).run()["bad"])
        show(shrink(sd, m), m, sd)
    elif a[0] == "run":
        r = Sim(int(a[1]), a[2] if len(a) > 2 else None).run()
        [print(f"{k:<10}{v}") for k, v in r.items() if k not in ("lin_ops",)]
    else: print(__doc__)
