;; Copyright (c) 2010 Tunde Ashafa
;; All rights reserved.

;; Redistribution and use in source and binary forms, with or without
;; modification, are permitted provided that the following conditions
;; are met:
;; 1. Redistributions of source code must retain the above copyright
;;    notice, this list of conditions and the following disclaimer.
;; 2. Redistributions in binary form must reproduce the above copyright
;;    notice, this list of conditions and the following disclaimer in the
;;    documentation and/or other materials provided with the distribution.
;; 3. The name of the author may not be used to endorse or promote products
;;    derived from this software without specific prior written permission.

;; THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
;; IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
;; OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
;; IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
;; INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
;; NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
;; DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
;; THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
;; (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
;; THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


(ns #^{:author "Tunde Ashafa"}
  com.ashafa.nozzle
  (:use clojure.contrib.pprint)
  (:require [clojure.contrib.http.agent :as h]
            [clojure.contrib.duck-streams :as io])
  (:import  java.net.URLEncoder sun.misc.BASE64Encoder))

(def nozzles (ref {}))

(def twitter-api-version 1)

(declare spawn-hose)

(defn- basic-authentication
  "Encodes a username and password for Basic HTTP Authentication."
  [username password]
  (str "Basic " (.encode (BASE64Encoder.) (.getBytes (str username ":" password)))))

(defn- url-encode
  "Encodes a string to a valid URL string."
  [s]
  (.. URLEncoder (encode s) (replace "+" "%20")))

(defn- map-to-query-string
  "Takes a map and returns the url query string representation of the map."
  [m]
  (let [kws (keys m)]
    (reduce 
     (fn [q kw]
       (let [k (if (keyword? kw) (name kw) kw)
             v (str (m kw))
             a (if(not= (last kws) kw) "&")]
         (str q (url-encode k) "=" (url-encode v) a))) "" kws)))

(defn- http-agent-handler
  "HTTP agent handler. Runs the nozzle callback, with each line from the stream as the single argument, in a different
   thread. Also sends a message to close other connections once a succesful newer connection is made."
  [nozzle-id spawn-time agnt]
  (send ((@nozzles nozzle-id) spawn-time) assoc :status ::closing)
  (if (h/success? agnt)
    (let [nozzle            (@nozzles nozzle-id)
          this-hose         (nozzle spawn-time)
          active-spawn-time (nozzle :active-spawn-time)
          previous-hose     (nozzle active-spawn-time)
          is-old-spawn?     (and previous-hose (< spawn-time active-spawn-time))]
      (when (and (not is-old-spawn?) (= (@this-hose :valve) ::open))
        (future
          (dosync
           (when previous-hose
             (send previous-hose assoc :status ::closing)
             (send previous-hose assoc :valve ::close))
           (send this-hose assoc :status ::opened)
           (send this-hose assoc :next-sleep 0)
       (alter nozzles assoc nozzle-id
                  (merge nozzle 
                         {:message "Streaming."
                          :active-spawn-time spawn-time
                          :spawning (not (= spawn-time (nozzle :latest-spawn-time)))}))))
        (loop [lines (io/read-lines (h/stream agnt))]
          (when (and (not-empty lines)
                     (= (:master-valve (@nozzles nozzle-id)) (@((@nozzles nozzle-id) spawn-time) :valve) ::open))
            (future (try
                     (((@nozzles nozzle-id) :nozzle-callback) (first lines))
                     (catch Exception e
                       (dosync (alter nozzles assoc-in [nozzle-id :callback-error] e)))))
            (recur (rest lines))))))))

(defn- get-http-agent
  "Returns an HTTP agent set up with a connection to the twitter streaming api."
  [nozzle-id spawn-time]
  (let [nozzle        (@nozzles nozzle-id)
        stream-method (nozzle :stream-method)
        method        (if (= stream-method "filter") "POST" "GET")
        query-string  (map-to-query-string (nozzle :parameters-map))
        url           (str "http://stream.twitter.com/" twitter-api-version "/statuses/" stream-method ".json" 
                           (if (= method "GET") (str "?" query-string)))
        body          (if (= method "POST") query-string)]
    (h/http-agent url
                  :method method
                  :body body
                  :headers {"Authorization" (basic-authentication nozzle-id (nozzle :password))}
                  :handler (partial http-agent-handler nozzle-id spawn-time))))

(defn- sleep-for
  "Calculate the next sleep duration depending on the last sleep duration and connection error."
  [current-sleep sleep-update-fn initial-sleep maximum-sleep]
  (if (= 0 current-sleep)
    initial-sleep
    (sleep-update-fn (min current-sleep maximum-sleep))))

(defn- hose-check
  "Everytime a connection (hose) changes its status, this function is run. Only when the connection status is 'closing' 
   this function closes the nozzle if it was intentionally closed, or it tries to reconnect to the twitter stream 
   (following the twitter streaming api guidelines on reconnecting) if a network/HTTP error occurs."
  [nozzle-id hose old-state new-state]
  (if (= (new-state :status) ::closing)
    (let [nozzle       (@nozzles nozzle-id)
          master-valve (nozzle :master-valve)
          valve        (new-state :valve)
          spawn-time   (new-state :spawn-time)]
      (dosync 
       (alter nozzles assoc nozzle-id (dissoc nozzle spawn-time)))
      (if (= master-valve valve ::open)
        (let [http-agent      (new-state :http-agent)
              http-error      (h/error? http-agent)
              sleep-signature (if http-error
                                [(partial * 2) 10000 240000]
                                [(partial + 250) 0 16000])]
          (if (or http-error (and (not (nozzle :spawning)) (= spawn-time (nozzle :active-spawn-time))))
            (spawn-hose nozzle-id
                        (if http-error
                          (str (h/status http-agent) " - " (h/message http-agent) ".")
                          "Network error.")
                        (apply sleep-for (new-state :next-sleep) sleep-signature))
            (dosync
             (alter nozzles assoc-in [nozzle-id :message] "Streaming.")))))
      (if (= master-valve ::close)
        (dosync
         (alter nozzles assoc-in [nozzle-id :message] "Stream closed."))))))

(defn- spawn-hose
  "Setup a new connection for the given nozzle-id and watch the connection state for any changes with the 'hose-check' function."
  ([nozzle-id]
     (spawn-hose nozzle-id "No reason given."))
  ([nozzle-id reason]
     (spawn-hose nozzle-id reason 0))
  ([nozzle-id reason sleep]
     (spawn-hose nozzle-id reason sleep nil))
  ([nozzle-id reason sleep next-sleep]
     (future
       (dosync
        (alter nozzles assoc-in [nozzle-id :message] (str "Waiting: " reason)))
       (let [nozzle (@nozzles nozzle-id)]
         (when (not (nozzle :sleep))
           (dosync
            (alter nozzles assoc-in [nozzle-id :sleep] sleep))
           (Thread/sleep sleep)
           (dosync
            (let [master-valve (:master-valve nozzle)
                  spawn-time   (.getTime (java.util.Date.))]
              (if (= ::open master-valve)
                (alter nozzles assoc nozzle-id
                       (merge (@nozzles nozzle-id)
                              {:sleep nil
                               :latest-spawn-time spawn-time
                               :spawning true
                               :message (str "Establishing new connection after: " reason)
                               spawn-time (add-watch 
                                           (agent {:next-sleep (or next-sleep sleep)
                                                   :spawn-time spawn-time
                                                   :status ::opening
                                                   :valve ::open
                                                   :http-agent (get-http-agent nozzle-id spawn-time)}) 
                                           nozzle-id hose-check)}))
                (alter nozzles assoc-in [nozzle-id :message] (str "Stream closed: " reason))))))))))
   
;; API ...

(defn create-nozzle
  "Create a new nozzle with a twitter screen name and password  (the twitter screen name is used as the nozzle id)."
  ([stream-method username password nozzle-callback]
     (create-nozzle stream-method username password nozzle-callback nil))
  ([stream-method username password nozzle-callback parameters-map]
     {:pre [(not (contains? @nozzles username)) (fn? nozzle-callback)]}
     (dosync
      (alter nozzles assoc username
             {:stream-method stream-method
              :parameters-map parameters-map
              :username username
              :password password
              :master-valve ::open
              :nozzle-callback nozzle-callback}))
     (spawn-hose username "New connection request.")
     {:nozzle-id username :stream-method stream-method}))


(defn nozzle-status
  "Displays the state of all nozzles or if a nozzle-id is provided, the state of a single nozzle."
  ([]
     (pprint @nozzles))
  ([nozzle-id]
     (if-let [nozzle (@nozzles nozzle-id)]
       (let [parameters (nozzle :parameters-map)
             wake       (and (nozzle :latest-spawn-time) (nozzle :sleep))
             status     {:status (nozzle :message)}
             status-w-p (if parameters 
                          (assoc status :parameters parameters))
             status-w-w (if wake
                          (assoc (or status-w-p status) :retrying-in
                                 (str "~ " 
                                      (int (/ (- (+ (nozzle :latest-spawn-time)
                                                    (nozzle :sleep))
                                                 (.getTime (java.util.Date.)))
                                              1000))
                                      "s")))]
         (or status-w-w status-w-p status)))))
  

(defn update-password
  "Changes the password of a nozzle."
  [nozzle-id password]
  {:pre [(@nozzles nozzle-id)]}
  (dosync 
   (alter nozzles assoc-in [nozzle-id :password] password))
  (spawn-hose nozzle-id "Password update request." 1000 0)
  (nozzle-status nozzle-id))

(defn update-stream
  "Update the streaming method or method parameters. A new connection (hose) with the new parameters to the streaming
   api is initiated and the old connection is only dropped after the new connection has connected successfully."
  ([nozzle-id parameters-map]
     {:pre [(@nozzles nozzle-id)]}
     (update-stream nozzle-id parameters-map ((@nozzles nozzle-id) :stream-method)))
  ([nozzle-id parameters-map stream-method]
     {:pre [(@nozzles nozzle-id)]}
     (dosync
      (alter nozzles assoc-in [nozzle-id :stream-method] stream-method)
      (alter nozzles assoc-in [nozzle-id :parameters-map] parameters-map))
     (spawn-hose nozzle-id "Stream update request." 1000 0)
     (nozzle-status nozzle-id)))

(defn update-callback
  "Changes the callback of a nozzle."
  [nozzle-id callback]
  {:pre [(@nozzles nozzle-id) (fn? callback)]}
  (dosync
   (alter nozzles assoc-in [nozzle-id :nozzle-callback] callback))
  (nozzle-status nozzle-id))


(defn open-nozzle
  "Opens a closed nozzle."
  [nozzle-id]
  {:pre [(@nozzles nozzle-id)]}
  (dosync
   (alter nozzles assoc-in [nozzle-id :master-valve] ::open))
  (spawn-hose nozzle-id "Open nozzle request." 1000)
  (nozzle-status nozzle-id))

(defn close-nozzle
  "Closes an open nozzle."
  [nozzle-id]
  {:pre [(@nozzles nozzle-id)]}
  (dosync
   (alter nozzles assoc-in [nozzle-id :master-valve] ::close))
  (nozzle-status nozzle-id))