(require '[metrics-galaxy.event-record :as er])

(defn process-line
  [line]
  (let [events (clojure.string/split line #"\t")
        [referral lead booking] (map #(when (not (empty? %)) (er/string->event-record %)) events)
        defaults [::er/visitor-id ::er/event-id ::er/ts ::er/type]
        referral (select-keys referral
                              (concat defaults
                                      [::er/location-tid ::er/partner-code ::er/udicode ::er/check-in ::er/check-out]))
        lead (when (not (empty? lead))
               (-> lead
                   (assoc ::er/referral-id (::er/event-id referral))
                   (select-keys (concat defaults [::er/rate-partner]))))
        booking (when (not (empty? booking))
                  (-> booking
                      (assoc ::er/lead-id (::er/event-id lead))
                      (select-keys (concat defaults [::er/check-in ::er/check-out ::er/nights ::er/rooms ::er/total-amount
                                                     ::er/currency ::er/rate-partner]))))
        line [referral lead booking]]
    (str (clojure.string/join "\t" (map er/event-record->string line)) \newline)))

(defn save-lines
  [writer lines]
  (.write writer (apply str lines)))


(def input "Set the input file" "/home/shewitt/data/challenge-raw.tsv")
(def output "Set the output file" "/home/shewitt/data/challenge.tsv")
(def batch-size "Set the size of write batches" 1000)


(do
  (spit output "" :append false)
  (def lines (map process-line (line-seq (clojure.java.io/reader input))))
  (with-open [writer (clojure.java.io/writer output)]
    (.write writer (str "referral" \tab "lead" \tab "booking" \newline))
    (doseq [batch (partition-all batch-size lines)]
      (save-lines writer batch))))

