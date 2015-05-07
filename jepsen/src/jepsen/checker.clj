(ns jepsen.checker
  "Validates that a history is correct with respect to some model."
  (:refer-clojure :exclude [set])
  (:require [clojure.stacktrace :as trace]
            [clojure.core :as core]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [jepsen.util :as util]
            [multiset.core :as multiset]
            [gnuplot.core :as g]
            [knossos.core :as knossos]
            [knossos.history :as history]))

(defprotocol Checker
  (check [checker test model history]
         "Verify the history is correct. Returns a map like

         {:valid? true}

         or

         {:valid?    false
          :failed-at [details of specific operations]}

         and maybe there can be some stats about what fraction of requests
         were corrupt, etc."))

(defn check-safe
  "Like check, but wraps exceptions up and returns them as a map like

  {:valid? nil :error \"...\"}"
  [checker test model history]
  (try (check checker test model history)
       (catch Throwable t
         {:valid? false
          :error (with-out-str (trace/print-cause-trace t))})))

(def unbridled-optimism
  "Everything is awesoooommmmme!"
  (reify Checker
    (check [this test model history] {:valid? true})))

(def linearizable
  "Validates linearizability with Knossos."
  (reify Checker
    (check [this test model history]
      (knossos/analysis model history))))

(def queue
  "Every dequeue must come from somewhere. Validates queue operations by
  assuming every non-failing enqueue succeeded, and only OK dequeues succeeded,
  then reducing the model with that history. Every subhistory of every queue
  should obey this property. Should probably be used with an unordered queue
  model, because we don't look for alternate orderings. O(n)."
  (reify Checker
    (check [this test model history]
      (let [final (->> history
                       (r/filter (fn select [op]
                                   (condp = (:f op)
                                     :enqueue (knossos/invoke? op)
                                     :dequeue (knossos/ok? op)
                                     false)))
                                 (reduce knossos/step model))]
        (if (knossos/inconsistent? final)
          {:valid? false
           :error  (:msg final)}
          {:valid?      true
           :final-queue final})))))

(def set
  "Given a set of :add operations followed by a final :read, verifies that
  every successfully added element is present in the read, and that the read
  contains only elements for which an add was attempted."
  (reify Checker
    (check [this test model history]
      (let [attempts (->> history
                          (r/filter knossos/invoke?)
                          (r/filter #(= :add (:f %)))
                          (r/map :value)
                          (into #{}))
            adds (->> history
                      (r/filter knossos/ok?)
                      (r/filter #(= :add (:f %)))
                      (r/map :value)
                      (into #{}))
            final-read (->> history
                          (r/filter knossos/ok?)
                          (r/filter #(= :read (:f %)))
                          (r/map :value)
                          (reduce (fn [_ x] x) nil))]
        (if-not final-read
          {:valid? false
           :error  "Set was never read"})

        (let [; The OK set is every read value which we tried to add
              ok          (set/intersection final-read attempts)

              ; Unexpected records are those we *never* attempted.
              unexpected  (set/difference final-read attempts)

              ; Lost records are those we definitely added but weren't read
              lost        (set/difference adds final-read)

              ; Recovered records are those where we didn't know if the add
              ; succeeded or not, but we found them in the final set.
              recovered   (set/difference ok adds)]

          {:valid?          (and (empty? lost) (empty? unexpected))
           :ok              (util/integer-interval-set-str ok)
           :lost            (util/integer-interval-set-str lost)
           :unexpected      (util/integer-interval-set-str unexpected)
           :recovered       (util/integer-interval-set-str recovered)
           :ok-frac         (util/fraction (count ok) (count attempts))
           :unexpected-frac (util/fraction (count unexpected) (count attempts))
           :lost-frac       (util/fraction (count lost) (count attempts))
           :recovered-frac  (util/fraction (count recovered) (count attempts))})))))

(defn fraction
  "a/b, but if b is zero, returns unity."
  [a b]
  (if (zero? b)
           1
           (/ a b)))

(def total-queue
  "What goes in *must* come out. Verifies that every successful enqueue has a
  successful dequeue. Queues only obey this property if the history includes
  draining them completely. O(n)."
  (reify Checker
    (check [this test model history]
      (let [attempts (->> history
                          (r/filter knossos/invoke?)
                          (r/filter #(= :enqueue (:f %)))
                          (r/map :value)
                          (into (multiset/multiset)))
            enqueues (->> history
                          (r/filter knossos/ok?)
                          (r/filter #(= :enqueue (:f %)))
                          (r/map :value)
                          (into (multiset/multiset)))
            dequeues (->> history
                          (r/filter knossos/ok?)
                          (r/filter #(= :dequeue (:f %)))
                          (r/map :value)
                          (into (multiset/multiset)))
            ; The OK set is every dequeue which we attempted.
            ok         (multiset/intersect dequeues attempts)

            ; Unexpected records are those we *never* attempted. Maybe
            ; duplicates, maybe leftovers from some earlier state. Definitely
            ; don't want your queue emitting records from nowhere!
            unexpected (multiset/minus dequeues attempts)

            ; lost records are ones which we definitely enqueued but never
            ; came out.
            lost       (multiset/minus enqueues dequeues)

            ; Recovered records are dequeues where we didn't know if the enqueue
            ; suceeded or not, but an attempt took place.
            recovered  (multiset/minus ok enqueues)]

        {:valid?          (and (empty? lost) (empty? unexpected))
         :lost            lost
         :unexpected      unexpected
         :recovered       recovered
         :ok-frac         (util/fraction (count ok)         (count attempts))
         :unexpected-frac (util/fraction (count unexpected) (count attempts))
         :lost-frac       (util/fraction (count lost)       (count attempts))
         :recovered-frac  (util/fraction (count recovered)  (count attempts))}))))

(def counter
  "A counter starts at zero; add operations should increment it by that much,
  and reads should return the present value. This checker validates that at
  each read, the value is at greater than the sum of all :ok increments, and
  lower than the sum of all attempted increments.

  Note that this counter verifier assumes the value monotonically increases. If
  you want to increment by negative amounts, you'll have to recalculate and
  possibly widen the intervals for all pending reads with each invoke/ok write.

  Returns a map:

  {:valid?              Whether the counter remained within bounds
   :reads               [[lower-bound read-value upper-bound] ...]
   :errors              [[lower-bound read-value upper-bound] ...]
   :max-absolute-error  The [lower read upper] where read falls furthest outside
   :max-relative-error  Same, but with error computed as a fraction of the mean}
  "
  (reify Checker
    (check [this test model history]
      (loop [history            (seq (history/complete history))
             lower              0             ; Current lower bound on counter
             upper              0             ; Upper bound on counter value
             pending-reads      {}            ; Process ID -> [lower read-val]
             reads              []]           ; Completed [lower val upper]s
          (if (nil? history)
            ; We're done here
            (let [errors (remove (partial apply <=) reads)]
              {:valid?             (empty? errors)
               :reads              reads
               :errors             errors})
            ; But wait, there's more
            (let [op      (first history)
                  history (next history)]
              (case [(:type op) (:f op)]
                [:invoke :read]
                (recur history lower upper
                       (assoc pending-reads (:process op) [lower (:value op)])
                       reads)

                [:ok :read]
                (let [r (get pending-reads (:process op))]
                  (recur history lower upper
                         (dissoc pending-reads (:process op))
                         (conj reads (conj r upper))))

                [:invoke :add]
                (recur history lower (+ upper (:value op)) pending-reads reads)

                [:ok :add]
                (recur history (+ lower (:value op)) upper pending-reads reads)

                (recur history lower upper pending-reads reads))))))))

(defn compose
  "Takes a map of names to checkers, and returns a checker which runs each
  check (possibly in parallel) and returns a map of names to results; plus a
  top-level :valid? key which is true iff every checker considered the history
  valid."
  [checker-map]
  (reify Checker
    (check [this test model history]
      (let [results (->> checker-map
                         (pmap (fn [[k checker]]
                                 [k (check checker test model history)]))
                         (into {}))]
        (assoc results :valid? (every? :valid? (vals results)))))))

(defn latency-graph
  "Spits out graphs of latency to the given output directory."
  [dir]
  (reify Checker
    (check [this test model history]
      (let [; Function to split up a seq of ops into OK, failed, and crashed ops
            by-type (fn [ops]
                      {:ok   (filter #(= :ok   (:type (:completion %))) ops)
                       :fail (filter #(= :fail (:type (:completion %))) ops)
                       :info (filter #(= :info (:type (:completion %))) ops)})

            ; Function to extract a [time, latency] pair from an op
            point   #(list (double (util/nanos->secs (:time %)))
                           (double (util/nanos->ms   (:latency %))))

            ; Preprocess history
            history (util/history->latencies history)
            invokes (filter #(= :invoke (:type %)) history)

            ; Split up invocations by function, then ok/failed/crashed
            datasets (->> invokes
                          (group-by :f)
                          (util/map-kv (fn [[f ops]]
                                         [f (by-type ops)])))

            ; What functions/types are we working with?
            fs          (sort (keys datasets))
            types       [:ok :info :fail]

            ; How should we render types?
            types->points {:ok   1
                           :fail 2
                           :info 3}

            ; How should we render different fs?
            fs->colors  (->> fs
                             (map-indexed (fn [i f] [f i]))
                             (into {}))

            ; Extract nemesis start/stop pairs
            final-time  (->> history
                             rseq
                             (filter :time)
                             first
                             :time
                             util/nanos->secs
                             double)
            nemesis     (->> history
                             util/nemesis-intervals
                             (keep
                               (fn [[start stop]]
                                 (when start
                                   [(-> start :time util/nanos->secs double)
                                    (if stop
                                      (-> stop :time util/nanos->secs double)
                                      final-time)]))))]

        (g/raw-plot!
          (concat [[:set :output (str dir "/latency.png")]
                   [:set :term :png, :truecolor, :size (g/list 900 400)]]
                  '[[set title "Latency"]
                    [set autoscale]
                    [set xlabel "Time (s)"]
                    [set ylabel "Latency (ms)"]
                    [set key left top]
                    [set logscale y]]
                 ; Nemesis regions
                 (map (fn [[start stop]]
                        [:set :obj :rect
                         :from (g/list start [:graph 0])
                         :to   (g/list stop  [:graph 1])
                         :fillcolor :rgb "#000000"
                         :fillstyle :transparent :solid 0.05
                         :noborder])
                      nemesis)
                 ; Plot ops
                 [['plot (apply g/list
                                (for [f fs, t types]
                                  ["-"
                                   'with 'points
                                   'pointtype (types->points t)
                                   'linetype  (fs->colors f)
                                   'title (str (name f) " "
                                               (name t))]))]])
          (for [f fs, t types]
            (map point (get-in datasets [f t]))))

        {:valid? true
         :file   (str dir "/latency.png")}))))
