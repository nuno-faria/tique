(ns tique-checker.main
    (:require [elle.list-append :as elle])
    (:require [clojure.string :as str])
    (:require [cheshire.core :as json])
)

(defn write-to-json-file 
    [out seq] 
    (spit out 
        (apply str
            (interpose \newline 
                (map json/generate-string seq))))
)

(defn -main 
    ([] (prn "Missing arguments (generator <size> <out-json-file> | checker <history-json-file>)"))
    ([& args] (case (first args) 
        "generator" 
            (write-to-json-file 
                (nth args 2) 
                (take 
                    (Integer/parseInt (second args)) 
                    (elle/gen {:max-txn-length 5, :max-writes-per-key 32, :key-count 16})))
        "checker"
            (clojure.pprint/pprint 
                (elle/check 
                    {:directory "out" :cycle-search-timeout 10000 :consistency-models [:snapshot-isolation]} 
                    (map clojure.edn/read-string (str/split-lines (slurp (second args))))))
        (prn (str "Invalid argument: " (first args)))
    ))
)
