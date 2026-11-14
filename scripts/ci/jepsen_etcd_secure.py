#!/usr/bin/env python3
"""Edit jepsen-io/etcd's test so a run can be secured as TupleSky is.

    scripts/ci/jepsen_etcd_secure.py ETCD_TEST/src/jepsen/etcd

`jepsen_etcd.sh prepare` runs this for `etcd-version VERSION+tls` or
`VERSION+tls+auth`. The edits do nothing unless a run sets them going
(`jepsen_etcd.sh run` with ETCD_SECURE), so the plain etcd runs in the
same job run the test as it was:

* JEPSEN_ETCD_TLS, the runner's directory holding ca.crt, node.crt and
  node.key (`jepsen_etcd.sh certificates`): each member gets those files
  under /opt/etcd/tls and serves both planes over TLS, requiring a client
  certificate from its clients and its peers (--client-cert-auth,
  --peer-client-cert-auth). Every URL the test builds is https, and jetcd
  presents that certificate and trusts that CA.
* JEPSEN_ETCD_AUTH as well: once the members are up, the first one turns
  auth on with a root user and a user `jepsen` whose role grants
  read-write on the whole key space. The workload's clients log in as
  `jepsen`, which is not root, so etcd checks each request against the
  role's grant. The clients the test opens before that, or with no test
  (its nemeses' membership calls), send no credentials, so a secured run
  takes no faults.
"""
import sys

root = sys.argv[1]


def edit(path, edits):
    src = open(path).read()
    for old, new in edits:
        if src.count(old) != 1:
            sys.exit(f"{path} no longer has: {old!r}")
        src = src.replace(old, new)
    open(path, "w").write(src)


edit(f"{root}/support.clj", [(
    '  (str "http://" (c.net/ip node) ":" port))',
    '  (str (if (System/getenv "JEPSEN_ETCD_TLS") "https://" "http://")\n'
    '       (c.net/ip node) ":" port))',
)])

edit(f"{root}/db.clj", [
    ('(def pidfile (str dir "/etcd.pid"))\n',
     '(def pidfile (str dir "/etcd.pid"))\n'
     '(def tls-dir (str dir "/tls"))\n'
     '\n'
     '(defn tls-flags\n'
     '  "etcd\'s TLS flags for both planes, client certificates required on\n'
     '  each, when the run is secured (JEPSEN_ETCD_TLS)."\n'
     '  []\n'
     '  (when (System/getenv "JEPSEN_ETCD_TLS")\n'
     '    (let [ca  (str tls-dir "/ca.crt")\n'
     '          crt (str tls-dir "/node.crt")\n'
     '          k   (str tls-dir "/node.key")]\n'
     '      [:--cert-file crt :--key-file k :--trusted-ca-file ca :--client-cert-auth\n'
     '       :--peer-cert-file crt :--peer-key-file k :--peer-trusted-ca-file ca\n'
     '       :--peer-client-cert-auth])))\n'
     '\n'
     '(defn etcdctl-tls!\n'
     '  "etcdctl against this member over TLS, with its own certificate."\n'
     '  [node & args]\n'
     '  (c/su\n'
     '    (apply c/exec (str dir "/etcdctl")\n'
     '           :--endpoints (s/client-url node)\n'
     '           :--cacert (str tls-dir "/ca.crt")\n'
     '           :--cert (str tls-dir "/node.crt")\n'
     '           :--key (str tls-dir "/node.key")\n'
     '           args)))\n'
     '\n'
     '(defn enable-auth!\n'
     '  "Auth on: a root user, as etcd requires, and the user `jepsen`, not\n'
     '  root, whose role grants read-write on the whole key space (an empty\n'
     '  key with --prefix), so every request is checked against a grant."\n'
     '  [node]\n'
     '  (info node "enabling etcd auth: user jepsen, role jepsen read-write on every key")\n'
     '  (etcdctl-tls! node :user :add "root:jepsen-root")\n'
     '  (etcdctl-tls! node :user :grant-role :root :root)\n'
     '  (etcdctl-tls! node :role :add :jepsen)\n'
     '  (etcdctl-tls! node :role :grant-permission :jepsen :--prefix=true :readwrite "")\n'
     '  (etcdctl-tls! node :user :add "jepsen:jepsen")\n'
     '  (etcdctl-tls! node :user :grant-role :jepsen :jepsen)\n'
     '  (etcdctl-tls! node :auth :enable))\n'),
    ('         :--experimental-corrupt-check-time    "1m"])\n      )))',
     '         :--experimental-corrupt-check-time    "1m"])\n      (tls-flags)\n      )))'),
    ('          (cu/install-archive! url dir))))\n',
     '          (cu/install-archive! url dir))))\n'
     '\n'
     '    ; A secured run: the certificates the runner made, before etcd starts.\n'
     '    (when-let [local (System/getenv "JEPSEN_ETCD_TLS")]\n'
     '      (c/su (c/exec :mkdir :-p tls-dir))\n'
     '      (c/upload [(str local "/ca.crt") (str local "/node.crt") (str local "/node.key")]\n'
     '                tls-dir))\n'),
    ('    (jepsen/synchronize test)\n    (reset! (:initialized? test) true)',
     '    (jepsen/synchronize test)\n'
     '    (when (and (System/getenv "JEPSEN_ETCD_AUTH") (= node (first (:nodes test))))\n'
     '      (enable-auth! node))\n'
     '    (jepsen/synchronize test)\n'
     '    (reset! (:initialized? test) true)'),
])

edit(f"{root}/client.clj", [(
    '           b (if-let [r (:retry-max-attempts test)]\n'
    '               (.retryMaxAttempts b r)\n'
    '               b)\n',
    '           b (if-let [r (:retry-max-attempts test)]\n'
    '               (.retryMaxAttempts b r)\n'
    '               b)\n'
    '           ; A secured run (jepsen_etcd_secure.py): this certificate and CA.\n'
    '           b (if-let [d (System/getenv "JEPSEN_ETCD_TLS")]\n'
    '               (.sslContext b (reify Consumer\n'
    '                                (accept [_ sb]\n'
    '                                  (-> sb\n'
    '                                      (.trustManager (java.io.File. (str d "/ca.crt")))\n'
    '                                      (.keyManager (java.io.File. (str d "/node.crt"))\n'
    '                                                   (java.io.File. (str d "/node.key")))))))\n'
    '               b)\n'
    '           ; With auth, the workload\'s clients log in as jepsen, once the\n'
    '           ; members are up and auth is on.\n'
    '           b (if (and (System/getenv "JEPSEN_ETCD_AUTH")\n'
    '                      (some-> (:initialized? test) deref))\n'
    '               (-> b\n'
    '                   (.user (ByteSequence/from (.getBytes "jepsen" "UTF-8")))\n'
    '                   (.password (ByteSequence/from (.getBytes "jepsen" "UTF-8"))))\n'
    '               b)\n',
)])
print("etcd test secured: support.clj, db.clj, client.clj")
