(ns realtime-stats.core
  (:use [backtype.storm clojure config])
  (:import [backtype.storm StormSubmitter LocalCluster]
           (storm.kafka KafkaSpout
                       KafkaConfig
                       HostPort
                       SpoutConfig
                       StringScheme)
           (java.util ArrayList))
  (:gen-class))
  

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(defn createKafkaSpoutConfig
  []
  (let [
        stringScheme (StringScheme.) ;so the kafka spout reads things as strings we need to create the schema
        hostPort (HostPort. "127.0.0.1" 9092) 
        hostList (ArrayList.) 
        append  (.add hostList hostPort)
        staticHost (storm.kafka.KafkaConfig$StaticHosts. hostList 1)
        spoutConfig (SpoutConfig. staticHost "test-topic" "/zkRootFoo" "fooID")]
    (set! (. spoutConfig scheme) stringScheme) ;tell the spout to read strings
    spoutConfig))

(def kafkaSpout (KafkaSpout. (createKafkaSpoutConfig)))


(defspout sentence-spout ["sentence"]
  [conf context collector]
  (let [sentences ["a little brown dog"
                   "the man petted the dog"
                   "four score and seven years ago"
                   "an apple a day keeps the doctor away"]]
    (spout
     (nextTuple []
       (Thread/sleep 100)
       (emit-spout! collector [(rand-nth sentences)])         
       )
     (ack [id]
        ;; You only need to define this method for reliable spouts
        ;; (such as one that reads off of a queue like Kestrel)
        ;; This is an unreliable spout, so it does nothing here
        ))))

(defspout sentence-spout-parameterized ["word"] {:params [sentences] :prepare false}
  [collector]
  (Thread/sleep 500)
  (emit-spout! collector [(rand-nth sentences)]))

(defbolt split-sentence ["word"] [tuple collector]
  (let [words (.split (.getString tuple 0) " ")]
    (doseq [w words]
      (emit-bolt! collector [w] :anchor tuple))
    (ack! collector tuple)
    ))

(def a "asdf")
(str a "world")

(defbolt echo-sentence ["word"] [tuple collector]
  (let [word (.getString tuple 0)]
    (emit-bolt! collector [(str "hello there!" word)] :anchor tuple)
    (ack! collector tuple)))

(defbolt word-count ["word" "count"] {:prepare true}
  [conf context collector]
  (let [counts (atom {})]
    (bolt
     (execute [tuple]
       (let [word (.getString tuple 0)]
         (swap! counts (partial merge-with +) {word 1})
         (emit-bolt! collector [word (@counts word)] :anchor tuple)
         (ack! collector tuple)
         )))))

(defn mk-topology []

  (topology
   {"1" (spout-spec kafkaSpout)}
   {"5" (bolt-spec {"1" :shuffle}
                   echo-sentence
                   :p 2)}))

(defn run-local! []
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "word-count" {TOPOLOGY-DEBUG true} (mk-topology))
    (Thread/sleep 88000)
    (.shutdown cluster)
    ))

(defn submit-topology! [name]
  (StormSubmitter/submitTopology
   name
   {TOPOLOGY-DEBUG true
    TOPOLOGY-WORKERS 3}
   (mk-topology)))

(defn -main
  ([]
   (run-local!))
  ([name]
   (submit-topology! name)))


(comment


  )
