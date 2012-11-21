(ns realtime-stats.core
  :require [storm-kafka :as kafka-spout] )

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(comment

  (println "hello world")

  (require 'storm.kafka)
  (import 'SpoutConfig )

  (import 'storm.kafka.KafkaSpout)
  (import 'storm.kafka.KafkaSpout)
  (import 'storm.kafka.SpoutConfig)
  (import 'storm.kafka.HostPort)
  (import [storm.kafka.KafkaConfig])
  (import 'storm.kafka.KafkaConfig.StaticHosts)
  (import 'storm.kafka.BrokerHosts)

  (def kc (KafkaConfig/StaticHosts))
  (. KafkaConfig StaticHosts)
  (System/getProperty "java.vm.version")

  (def host (HostPort. "localhost" 9092))
  (def host-list '(host))
  (def broker (BrokerHosts. host 1))



  (SpoutConfig. host, "test" "/zkRoot" "fooid")

  (.StaticHosts KafkaConfig)

  (.fromHostString (.StaticHosts KafkaConfig) '("localhost"))
  (import 'ImmutableList)
  (.of ImmutableList "localhost")

  (SpoutConfig. )

  )
