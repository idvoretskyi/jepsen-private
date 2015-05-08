(ns jepsen.system.rdb
  (:require [clojure.tools.logging    :refer [debug info warn]]
            [clojure.java.io          :as io]
            [clojure.string           :as str]
            [jepsen.core              :as core]
            [jepsen.util              :refer [meh timeout]]
            [jepsen.codec             :as codec]
            [jepsen.core              :as core]
            [jepsen.control           :as c]
            [jepsen.control.net       :as net]
            [jepsen.control.util      :as cu]
            [jepsen.client            :as client]
            [jepsen.db                :as db]
            [jepsen.generator         :as gen]
            [jepsen.os.debian         :as debian]
            [knossos.core             :as knossos]
            [cheshire.core            :as json]
            [slingshot.slingshot      :refer [try+]]
            [rethinkdb.core :refer [connect close]]
            [rethinkdb.query :as r]))

(def binary "/usr/bin/rethinkdb")
(def pidfile "/var/run/rethinkdb.pid")
(def data-dir "/var/lib/rethinkdb")
(def log-file "/var/log/rethinkdb/log_file")

(defn running?
  "Is rethinkdb running?"
  []
  (try
    (c/exec :start-stop-daemon :--status
            :--pidfile pidfile
            :--exec binary)
    true
    (catch RuntimeException _ false)))

(defn start-rethinkdb!
  [test node]
  (info node "starting rethinkdb"))
  ;(c/exec binary
  ;        :-d data-dir
  ;        :--server-name (name node)
  ;        :--log-file log-file
  ;        :--pid-file pidfile
  ;        :--daemon
  ;        (c/lit "2>&1")))

(defn db []
  (let [running (atom nil)] ; A map of nodes to whether they're running
    (reify db/DB
      (setup! [this test node]
;        (c/su
;            ; Launch servers TODO!)
;
        (info node "rethinkdb ready"))

      (teardown! [_ test node]
;        (c/su
;          (meh (c/exec :killall :-9 :rethinkdb))
;          (c/exec :rm :-rf pidfile data-dir log-file))

        (info node "rethinkdb nuked")))))

(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
    (let [client (connect :host (name node) :port 28038)]
      (-> (r/table-create "cas") (r/run client))
      (-> (r/table "cas") (r/insert {:id k :val nil}) (r/run client))
      (assoc this :client client)))

  (invoke! [this test op]
    ; Reads are idempotent; if they fail we can always assume they didn't
    ; happen in the history, and reduce the number of hung processes, which
    ; makes the knossos search more efficient
    (let [fail (if (= :read (:f op))
                 :fail
                 :info)]
      (try+
        (case (:f op)
          :read  (let [value (-> (r/table "cas") (r/get k) (r/run client))]
                   (assoc op :type :ok :value value))

          :write (do (-> (r/table "cas")
                         (r/get k)
                         (r/replace (:value op))
                         (r/get-field :replaced)
                         (r/eq 1)
                         (r/run client))
                     (assoc op :type :ok))

          :cas   (let [[value value'] (:value op)
                       ok?            (-> (r/table "cas")
                                          (r/get k)
                                          (r/replace
                                            (r/fn [row] (r/branch (r/eq row value) value' row)))
                                          (r/get-field :replaced)
                                          (r/eq 1)
                                          (r/run client))]
                   (assoc op :type (if ok? :ok :fail))))

        ; TODO!
        ; A few common ways rethinkdb can fail
        (catch java.net.SocketTimeoutException e
          (assoc op :type fail :value :timed-out))

        (catch [:body "command failed to be committed due to node failure\n"] e
          (assoc op :type fail :value :node-failure))

        (catch [:status 307] e
          (assoc op :type fail :value :redirect-loop))

        (catch (and (instance? clojure.lang.ExceptionInfo %)) e
          (assoc op :type fail :value e))

        (catch (and (:errorCode %) (:message %)) e
          (assoc op :type fail :value e)))))

  (teardown! [_ test]))

(defn cas-client
  "A compare and set register built around a single rethinkdb node."
  []
  (CASClient. "jepsen" nil))
