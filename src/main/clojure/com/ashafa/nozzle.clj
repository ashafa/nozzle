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
  (:require [clojure.contrib.http.agent :as h]
            [clojure.contrib.duck-streams :as io])
  (:import  java.net.URLEncoder sun.misc.BASE64Encoder))

(declare spawn-hose)

(def nozzles (ref {}))

(def twitter-api-version 1)

(defn- basic-authentication
  "Encodes a username and password for Basic HTTP Authentication."
  [username password]
  (str "Basic " (.encode (BASE64Encoder.) (.getBytes (str username ":" password)))))

(defn- url-encode
  "Encodes a string to a valid URL string."
  [s]
  (.. URLEncoder (encode s) (replace "+" "%20")))

(defn- toggle
  "Takes 3 arguments and returns the third argument if the first and second argument are equal, else returns the second
   argument."
  [current-option first-option second-option]
  (if (= current-option first-option)
    second-option first-option))

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
  "HTTP agent handler."
  [nozzle-id hose-id agnt]
  (send ((@nozzles nozzle-id) hose-id) assoc :status ::closing)
  (if (h/success? agnt)
    (let [nozzle        (@nozzles nozzle-id)
          hose          (nozzle hose-id)
          other-hose-id (toggle hose-id :hose-1 :hose-2)
          other-hose    (nozzle other-hose-id)]
      (send hose assoc :sleep nil)
      (future (send hose assoc :status ::opened)
              (when other-hose
                (dosync (alter nozzles assoc-in [nozzle-id :active-hose-id] hose-id))
                (send other-hose assoc :valve ::close)))
      (loop [lines (io/read-lines (h/stream agnt))]
        (when (and (not (empty? lines))
                   (= (:master-valve (@nozzles nozzle-id)) (@hose :valve) ::open))
          (future (try
                   (((@nozzles nozzle-id) :nozzle-callback) (first lines))
                   (catch Exception e
                     (dosync (alter nozzles assoc-in [nozzle-id :callback-error] e)))))
          (recur (rest lines)))))))

(defn- get-http-agent
  "Returns an HTTP agent set up with a connection to the twitter streaming api."
  [hose-id nozzle-id]
  (let [nozzle        (@nozzles nozzle-id)
        stream-method (nozzle :stream-method)
        method        (if (= stream-method "filter") "POST" "GET")
        query-string  (map-to-query-string (nozzle :parameters-map))
        url           (str "http://stream.twitter.com/" twitter-api-version "/statuses/" stream-method ".json" 
                           (if (= method "GET") (str "?" query-string)))
        body          (if (= method "POST") query-string)]
    (h/http-agent url
                  :method  method
                  :body    body
                  :headers {"Authorization" (basic-authentication nozzle-id (nozzle :password))}
                  :handler (partial http-agent-handler nozzle-id hose-id))))

(defn- sleep-for
  "Causes the current thread to sleep before creating a new connection, when used with the 'spawn-nozzle' fn, and 
   returns the length of the next sleep duration of the nozzle if an error occurs during the new connection."
  [current-sleep sleep-update-fn initial-sleep maximum-sleep]
  (let [sleep (min (or current-sleep initial-sleep) maximum-sleep)]
    (Thread/sleep sleep)
    (sleep-update-fn sleep)))

(defn- hose-check
  "Everytime a connection (hose) changes its status, this function is run. Only when the connection status is 'closing', 
   closes the nozzle if it was intentionally closed, or tries to reconnect to twitter (following the twitter streaming api
   guidelines on reconnecting) if a network/HTTP error occurs."
  [nozzle-id hose old-state new-state]
  (let [nozzle                     (@nozzles nozzle-id)
        other-hose                 (nozzle (toggle (new-state :hose-id) :hose-1 :hose-2))
        valve                      (new-state :valve)
        [other-status other-valve] (if other-hose [(@other-hose :status) (@other-hose :valve)])]
    (if (= (new-state :status) ::closing)
      (cond (= (nozzle :master-valve) ::close)
            (send hose assoc :status ::closed)
            (and (= valve ::open)
                 (or (= other-status ::changing) (not= ::open other-valve)))
            (let [sleep-signature (if (h/error? (new-state :http-agent))
                                    [(partial * 2) 10000 240000]
                                    [(partial + 250) 0 16000])]
              (spawn-hose (new-state :hose-id) nozzle-id
                            (apply sleep-for (new-state :sleep) sleep-signature)))
            (= other-valve valve ::open)
            (send hose assoc :status ::changing)
            :else (send hose assoc :status ::closed)))))

(defn- spawn-hose
  "Setup a new connection and its state (hose), watch the hose with the 'hose-watch' function, and associate it 
   with a nozzle."
  [hose-id nozzle-id next-sleep]
  (let [hose-state     {:http-agent (get-http-agent hose-id nozzle-id)
                        :hose-id    hose-id
                        :sleep      next-sleep
                        :wake       (+ (.getTime (java.util.Date.)) (or next-sleep 10000))
                        :status     ::opening
                        :valve      ::open}
        old-hose       ((@nozzles nozzle-id) hose-id)
        new-hose-state (merge (if old-hose @old-hose) (dissoc hose-state (if next-sleep :valve)))]
    (dosync
     (alter nozzles assoc-in [nozzle-id hose-id]
            (add-watch (agent new-hose-state) nozzle-id hose-check)))))

;; api...

(defn create-nozzle
  "Create a new nozzle with a twitter screen name and password  (the twitter screen name is used as the nozzle id)."
  ([stream-method username password nozzle-callback]
     (create-nozzle stream-method username password nozzle-callback nil))
  ([stream-method username password nozzle-callback parameters-map]
     {:pre [(and (not (contains? @nozzles username)) (fn? nozzle-callback))]}
     (dosync
      (alter nozzles assoc username
             {:stream-method   stream-method
              :parameters-map  parameters-map
              :username        username
              :password        password
              :active-hose-id  :hose-1
              :master-valve    ::open
              :nozzle-callback nozzle-callback})
      (spawn-hose :hose-1 username nil))
     {:nozzle-id username :stream-method stream-method}))

(defn get-nozzle-status
  "Get the current status of a nozzle."
  [nozzle-id]
  (if-let [nozzle (@nozzles nozzle-id)]
    (let [active-hose-id            (nozzle :active-hose-id)
          hose                      (nozzle active-hose-id)
          other-hose-id             (toggle active-hose-id :hose-1 :hose-2)
          other-hose                (nozzle other-hose-id)
          master-valve              (nozzle :master-valve)
          status                    (@hose :status)
          agnt                      (@hose :http-agent)
          valve                     (@hose :valve)
          [other-status other-agnt] (if other-hose [(@other-hose :status) (@other-hose :http-agent)])
          message                   (cond (and (#{master-valve valve} ::close)
                                               (not= status ::closed))
                                          "Closing stream..."
                                          (= status ::opening)
                                          "Waiting for stream..."
                                          (and (= status ::changing)
                                               (= other-status ::opening))
                                          "Streaming with previous parameters. Changing stream..."
                                          (and (= status ::changing)
                                               (h/error? other-agnt))
                                          (str "Streaming with previous parameters. Error changing stream ("  
                                               (h/status other-agnt) " - " (h/message other-agnt) 
                                               "). Retrying in " (int (/ (- (@other-hose :wake) (.getTime (java.util.Date.))) 1000)) "s.")
                                          (and (h/error? agnt)
                                               (= status ::closed))
                                          (str "Stream closed with error (" (h/status agnt) " - " (h/message agnt) ").")
                                          (h/error? agnt)
                                          (str "Error opening stream (" (h/status agnt) " - " (h/message agnt) "). Retrying in " 
                                               (int (/ (- (@hose :wake) (.getTime (java.util.Date.))) 1000)) "s.")
                                          (h/done? agnt)
                                          "Stream closed." :else "Streaming!")]
      {:message        message
       :nozzle-id      nozzle-id
       :stream-method  (nozzle :stream-method)
       :parameters-map (nozzle :parameters-map)
       :status         status})))

(defn update-nozzle-stream
  "Update the streaming method or method parameters. A new connection (hose) with the new parameters to the streaming
   api is initiated and the old connection is only dropped after the new connection with the new parameters has
   connected successfully."
  ([nozzle-id parameters-map]
     (if-let [nozzle (@nozzles nozzle-id)]
       (update-nozzle-stream nozzle-id parameters-map (nozzle :stream-method))))
  ([nozzle-id parameters-map stream-method]
     (if-let [nozzle (@nozzles nozzle-id)]
       (let [active-hose-id (nozzle :active-hose-id)
             hose           (nozzle active-hose-id)
             other-hose-id  (toggle active-hose-id :hose-1 :hose-2)
             other-hose     (nozzle other-hose-id)
             status         (@hose :status)]
         (cond (= status ::opened)
               (dosync
                (alter nozzles assoc-in [nozzle-id :stream-method] stream-method)
                (alter nozzles assoc-in [nozzle-id :parameters-map] parameters-map)
                (send hose assoc :status ::changing)
                (spawn-hose other-hose-id nozzle-id nil))
               (and (#{::changing ::opening} status)
                    (or (not other-hose) (not (h/error? (@other-hose :http-agent)))))
               (throw (IllegalStateException. "Can't update nozzle parameters at this time. Please try again later."))
               :else (dosync
                      (alter nozzles assoc-in [nozzle-id :stream-method] stream-method)
                      (alter nozzles assoc-in [nozzle-id :parameters-map] parameters-map)))
         (assoc (get-nozzle-status nozzle-id) :message "Updated nozzle parameters.")))))
  
(defn update-nozzle-password
  "Changes the password of a nozzle. The password can only be changed if a nozzle connection is either 'closing' (also the state 
   during an HTTP error) or 'closed'."
  [nozzle-id password]
  (if-let [nozzle (@nozzles nozzle-id)]
    (do
      (if (#{::closing ::closed} (@(nozzle (nozzle :active-hose-id)) :status))
        (dosync (alter nozzles assoc-in [nozzle-id :password] password))
        (throw (IllegalStateException. "Can't change nozzle password at the time. Please try again later.")))
      (assoc (get-nozzle-status nozzle-id) :message "Changed nozzle password."))))

(defn update-nozzle-callback
  "Changes the callback of a nozzle."
  [nozzle-id callback]
  {:pre [(fn? callback)]}
  (when (@nozzles nozzle-id)
    (dosync (alter nozzles assoc-in [nozzle-id :nozzle-callback] callback))
    (assoc (get-nozzle-status nozzle-id) :message "Changed nozzle callback.")))
  
(defn open-nozzle
  "Opens a closed nozzle."
  [nozzle-id]
  (if-let [nozzle (@nozzles nozzle-id)]
    (dosync
     (when (= (@(nozzle (nozzle :active-hose-id)) :status) ::closed)
       (alter nozzles assoc-in [nozzle-id :master-valve] ::open)
       (spawn-hose (nozzle :active-hose-id) nozzle-id nil))
     (assoc (get-nozzle-status nozzle-id) :message "Turning nozzle on."))))

(defn close-nozzle
  "Closes an open nozzle."
  [nozzle-id]
  (if-let [nozzle (@nozzles nozzle-id)]
    (dosync
     (alter nozzles assoc-in [nozzle-id :master-valve] ::close)
     (assoc (get-nozzle-status nozzle-id) :message "Turning nozzle off."))))