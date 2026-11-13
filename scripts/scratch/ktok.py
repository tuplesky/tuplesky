#!/usr/bin/env python3
"""The leader's (or a follower's) tokio thread split finer, µs a command: ktok.py OUT [L|F]
Each sample goes to the first category matching from its leaf up. On loopback the sender's
sendmsg also runs the receiver's UDP delivery and its wakeup; that is split out."""
import sys, re, collections, os
o = sys.argv[1]; side = sys.argv[2] if len(sys.argv) > 2 else 'L'
hz = os.sysconf('SC_CLK_TCK')
def load(f):
    r = {}
    for l in open(f):
        x = l.split(); r[(x[0], x[1])] = (x[2], int(x[4]) + int(x[5]))
    return r
a, b = load(o + '/s1.txt'), load(o + '/s2.txt')
lp, lm, fp, fm, lt, ft = open(o + '/roles.txt').read().split()
ap = {l.split()[0]: int(l.split()[2]) for l in open(o + '/a1.txt')}
bp = {l.split()[0]: int(l.split()[2]) for l in open(o + '/a2.txt')}
cmds = bp[lp] - ap[lp]
p, t = (lp, lt) if side == 'L' else (fp, ft)
us = (b[(p, t)][1] - a[(p, t)][1]) / hz / cmds * 1e6
CATS = [
 ('crypto: AEAD seal/open, header protection', r'aead|aes_gcm|gcm128|EVP_AEAD|cipher_new_mask|aes_hw|aesni|ghash|gcm_'),
 ('kernel: loopback delivery to the receiver', r'__netif_receive_skb|ip_rcv|ip_local_deliver|udp_queue_rcv|__udp_enqueue|sock_def_readable|ep_poll_callback|udp_unicast_rcv|process_backlog|net_rx_action|__wake_up'),
 ('kernel: UDP/IP send', r'udp_sendmsg|udp_send_skb|ip_send_skb|ip_output|ip_finish_output|__dev_queue_xmit|try_to_wake_up_x|ip_make_skb|__ip_append_data|ip_route_output|loopback_xmit|dev_hard_start_xmit|sock_alloc_send|udp4_lib'),
 ('kernel: sendmsg syscall itself', r'sys_sendmsg|__libc_sendmsg|sendmsg'),
 ('kernel: receive (recvmmsg)', r'recvmmsg|recvmsg|udp_recvmsg|skb_recv|sock_recvmsg|skb_datagram|skb_copy'),
 ('kernel: epoll wait', r'epoll_wait|do_epoll|ep_send_events|ep_poll|ep_item_poll'),
 ('kernel: futex', r'futex'),
 ('kernel: other syscall', r'do_syscall_64|entry_SYSCALL|syscall_exit|exit_to_user'),
 ('quinn-proto: connection, streams, packets', r'quinn_proto|proto::|PacketBuilder|packet_builder|Connection::|connection::|send_buffer|SendBuffer|recv_stream|send_stream|Assembler|Chunks|Recv::|Send::|spaces|PacketSpace|RangeSet|range_set'),
 ('quinn: drivers, endpoint, udp glue', r'quinn::|quinn_udp|ConnectionDriver|EndpointDriver|drive_transmit|drive_recv|RecvState|UdpSocketState|AsyncUdpSocket'),
 ('coord transport (lanes, frames)', r'coord_transport'),
 ('tokio runtime, wakers, channels', r'tokio|task::|harness|scheduler|Waker|wake|chan::|mpsc|AtomicWaker|Notify|Mutex'),
 ('alloc', r'mi_|malloc|free|realloc|alloc::'),
 ('memcpy/memmove', r'memcpy|memmove|memset'),
]
KERNEL = CATS[:8]; USER = CATS[8:]
ANY = [('crypto: AEAD seal/open, header protection', CATS[0][1])] + [(n, r) for n, r in CATS[1:8]]
c = collections.Counter(); tot = 0
for l in open(f'{o}/perf-{side}-tokio.folded'):
    st, n = l.rsplit(' ', 1); n = int(n); tot += n
    frames = st.split(';'); hit = None
    for name, rx in ANY:
        if any(re.search(rx, fr) for fr in frames): hit = name; break
    if not hit:
        for fr in reversed(frames):
            for name, rx in USER:
                if re.search(rx, fr): hit = name; break
            if hit: break
    c[hit or 'other'] += n
print(f'{o} {side}-tokio: {us:.1f} µs a command, {tot} samples, {cmds} commands')
for name, _ in CATS + [('other', '')]:
    if c[name]: print(f'  {us * c[name] / tot:6.1f}  {100 * c[name] / tot:5.1f}%  {name}')
