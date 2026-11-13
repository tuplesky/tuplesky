#!/usr/bin/env python3
"""Transport floor pairs: per run, the leader's tokio µs a command and its QUIC sends a command
by connection kind; each switch run against the mean of the base runs on either side.
tr_report.py CALLERS BASEPREFIX SWITCHPREFIX..."""
import sys, json, os, re, glob, collections
hz = os.sysconf('SC_CLK_TCK')
AR = re.compile(r'"application_records":(\d+)')
def load(f):
    r = {}
    for l in open(f):
        x = l.split(); r[(x[0], x[1])] = (x[2], int(x[4]) + int(x[5]))
    return r
def quic(path):
    rec = 0; out = []
    for l in open(path, errors='replace'):
        m = AR.search(l)
        if m: rec = int(m.group(1))
        if l.startswith('x-quic '):
            port = l.split(' ')[1].split('=')[1]
            kinds = {}
            for ch in re.finditer(r' (\S+): (\w+=\d+(?: \w+=\d+)*)', l.strip()):
                kinds[ch.group(1)] = {k: int(v) for k, v in re.findall(r'(\w+)=(\d+)', ch.group(2))}
            out.append((port, rec, kinds))
    sums = collections.defaultdict(lambda: collections.defaultdict(float))
    for port in sorted({p for p, _, _ in out}):
        L = [(r, k) for p, r, k in out if p == port]
        busy = [i for i in range(1, len(L)) if L[i][0] - L[i - 1][0] > 1000]
        if len(busy) < 3: continue
        (ra, ka), (rb, kb) = L[busy[0]], L[busy[-1]]
        for kind, x in kb.items():
            y = ka.get(kind, {})
            plane = kind.split('-')[0] + '-' + kind.split('-')[1]
            for f in x:
                if f == 'conns': continue
                sums[plane][f] += (x[f] - y.get(f, 0)) / (rb - ra)
    return sums
def row(o):
    a, b = load(o + '/s1.txt'), load(o + '/s2.txt')
    span = float(open(o + '/t2').read()) - float(open(o + '/t1').read())
    lp = open(o + '/roles.txt').read().split()[0]
    a1 = {l.split()[0]: (l.split()[1], int(l.split()[2])) for l in open(o + '/a1.txt')}
    a2 = {l.split()[0]: (l.split()[1], int(l.split()[2])) for l in open(o + '/a2.txt')}
    cmds = a2[lp][1] - a1[lp][1]
    bench = json.load(open(o + '/bench.json'))
    okps = bench['achieved']['achieved_per_second'].get('Observed') or 0
    ops = okps * span
    tok = collections.defaultdict(float); servers = 0.0
    for (p, t), (c, v) in b.items():
        if (p, t) not in a: continue
        cpu = (v - a[(p, t)][1]) / hz; servers += cpu
        if c.startswith('tokio'): tok['L' if p == lp else 'F'] += cpu
    n = 0; m = 0.0
    for path in bench['paths'].values():
        sv = path['service']; k = sv['count']; n += k; m += sv['mean_ns'] * k
    q = quic(o + '/' + a1[lp][0] + '-coordd.log')
    r = collections.OrderedDict()
    r['ops/s'] = okps; r['cmds/op'] = cmds / ops
    r['servers ms/op'] = servers / ops * 1e3
    r['L tokio us/cmd'] = tok['L'] / cmds * 1e6
    r['F tokio us/cmd'] = tok['F'] / 4 / cmds * 1e6
    r['lat ms'] = m / n / 1e6
    for plane, short in [('Peer-Voter', 'peer'), ('Api-Frontend', 'apiF'), ('Api-Client', 'cli')]:
        s = q.get(plane, {})
        r[short + ' sends'] = s.get('tx_ios', 0)
        r[short + ' dgr'] = s.get('tx_dgrams', 0)
        r[short + ' rx'] = s.get('rx_dgrams', 0)
        r[short + ' acks'] = s.get('tx_acks', 0)
    r['L sends/cmd'] = sum(s.get('tx_ios', 0) for s in q.values())
    r['L dgr tx+rx'] = sum(s.get('tx_dgrams', 0) + s.get('rx_dgrams', 0) for s in q.values())
    return r
c = int(sys.argv[1]); bpre = sys.argv[2]; pres = sys.argv[3:]
runs = [x for x in os.listdir('.') if x.endswith(f'-c{c}') and os.path.exists(x + '/cost.json') and (x.startswith(bpre) or any(x.startswith(p) for p in pres))]
order = sorted(runs, key=lambda x: os.path.getmtime(x + '/bench.json'))
rows = {x: row(x) for x in order}
keys = list(next(iter(rows.values())).keys())
print(f'{"run":16s}' + ''.join(f'{k[:13]:>14s}' for k in keys))
for x in order:
    print(f'{x:16s}' + ''.join(f'{rows[x][k]:14.3f}' for k in keys))
bases = [x for x in order if x.startswith(bpre)]
print(f'{"base mean":16s}' + ''.join(f'{sum(rows[x][k] for x in bases)/len(bases):14.3f}' for k in keys) + f'  ({len(bases)})')
for p in pres:
    diffs = []
    for i, x in enumerate(order):
        if not x.startswith(p): continue
        nb = [y for y in order[max(0, i - 1):i] + order[i + 1:i + 2] if y.startswith(bpre)]
        if not nb: continue
        diffs.append({k: rows[x][k] - sum(rows[y][k] for y in nb) / len(nb) for k in keys})
    if diffs:
        print(f'{p+" vs bases":16s}' + ''.join(f'{sum(d[k] for d in diffs)/len(diffs):+14.3f}' for k in keys) + f'  ({len(diffs)}: ' + ' '.join(f'{d["L tokio us/cmd"]:+.1f}' for d in diffs) + ')')
