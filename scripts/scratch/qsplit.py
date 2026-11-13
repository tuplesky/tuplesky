#!/usr/bin/env python3
"""QUIC counts by connection kind, per op, from the x-quic lines (COORDD_X_QSPLIT=1).
Each x-quic line is cumulative over live connections; the window is two lines inside the
measured phase, read against the node's own application_records at each line, and turned
into per-op figures with the run's ops per application record at the leader."""
import sys, re, json, glob, os
AR = re.compile(r'"application_records":(\d+)')
def node_lines(path):
    rec = 0; out = []
    for l in open(path, errors='replace'):
        m = AR.search(l)
        if m: rec = int(m.group(1))
        if l.startswith('x-quic '):
            parts = l.strip().split(' ')
            port = parts[1].split('=')[1]
            kinds = {}
            body = l.strip()[len('x-quic port=' + port):]
            for chunk in re.finditer(r' (\S+): (\w+=\d+(?: \w+=\d+)*)', body):
                kinds[chunk.group(1)] = {k: int(v) for k, v in re.findall(r'(\w+)=(\d+)', chunk.group(2))}
            out.append((port, rec, kinds))
    return out, rec
def window(lines, port):
    L = [(r, k) for p, r, k in lines if p == port]
    # busy lines: the record count moved since the line before
    busy = [i for i in range(1, len(L)) if L[i][0] - L[i - 1][0] > 1000]
    if len(busy) < 3: return None
    a, b = L[busy[0]], L[busy[-1] - 0]
    return a, b
d = sys.argv[1]
b = json.load(open(d + '/bench.json')); ops = b['schedule']['warmup_ops'] + b['schedule']['measured_ops']
nodes = {}
for f in sorted(glob.glob(d + '/n*-coordd.log')):
    lines, last = node_lines(f); nodes[os.path.basename(f)[:2]] = (lines, last)
for n, (lines, last) in nodes.items():
    ports = sorted({p for p, _, _ in lines})
    for port in ports:
        w = window(lines, port)
        if not w: print(n, port, 'no window'); continue
        (ra, ka), (rb, kb) = w
        drec = rb - ra; opr = ops / last if last else 0
        for kind in sorted(kb):
            x = kb[kind]; y = ka.get(kind, {})
            dd = {k: (x[k] - y.get(k, 0)) for k in x}
            per = {k: v / drec / opr if drec and opr else 0 for k, v in dd.items()}
            print(f"{n} port={port} {kind:28s} conns={x['conns']} per-op: "
                  + ' '.join(f"{k}={per[k]:.3f}" for k in ['tx_ios', 'tx_dgrams', 'sent_packets', 'tx_acks', 'tx_stream', 'tx_dgram_frames', 'tx_ping', 'tx_flow', 'tx_crypto', 'tx_other', 'rx_ios', 'rx_dgrams', 'rx_acks', 'lost_packets'])
                  + f" bytes/dgram={dd['tx_bytes']/dd['tx_dgrams'] if dd['tx_dgrams'] else 0:.0f} dgrams/io={dd['tx_dgrams']/dd['tx_ios'] if dd['tx_ios'] else 0:.2f}"
                  + f"  [records {drec}, ops/record {opr:.3f}]")
