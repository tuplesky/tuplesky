;; tuplesky/jepsen#8's two snippets, verbatim, each run on its own so one
;; failing can't hide the other. Run from a jepsen checkout's jepsen/
;; project with `lein run -m clojure.main` (scripts/ci/jepsen_fork_tests).

(defmacro snippet [title & body]
  `(do (println "=== snippet" ~title)
       (try ~@body
            (catch Throwable t#
              (println "threw:" (.getName (class t#)) (.getMessage t#))))
       (flush)))

(snippet 1
  (require '[jepsen.generator :as gen]
           '[jepsen.generator.context :as ctx])
  (eval
    '(let [all   (gen/context {:concurrency 30})          ; threads 0-29 + nemesis
           group ((ctx/make-thread-filter (set (range 20 30))) all) ; what op hands group 2
           g     (gen/process-limit 20 (repeat {:f :write, :value 1}))]
       ;; Asked first in its group's context, as op does: an op on a thread 20-29.
       (prn (first (gen/op g {} group)))
       ;; Updated first in the whole test's context, as ConcurrentGenerator's
       ;; update does: the filter keeps threads 0-19, so group 2 gets :pending
       ;; for ever.
       (prn (-> g
                (gen/update {} all {:type :ok, :process 25, :f :write, :time 0})
                (gen/op {} group)
                first)))))

(snippet 2
  (require '[jepsen.generator :as gen]
           '[jepsen.independent :as independent])
  (eval
    '(let [seen  (atom [])
           rec   (fn rec [_k]
                   (reify gen/Generator
                     (op [this test ctx]
                       (let [[op _] (gen/op {:f :write, :value 1} test ctx)]
                         [op this]))
                     (update [this test ctx event]
                       (swap! seen conj (sort (gen/all-threads ctx)))
                       this)))
           all   (gen/context {:concurrency 30})
           [_ g] (gen/op (independent/concurrent-generator 10 (range) rec) {} all)]
       (gen/update g {} all {:type :ok, :process 25, :f :write, :time 0,
                             :value (independent/tuple 2 1)})
       (prn @seen))))

(shutdown-agents)
