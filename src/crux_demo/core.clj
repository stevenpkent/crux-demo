(ns crux-demo.core
  (:gen-class)
  (:require [crux.api :as crux]))

(def manifest {:crux.db/id :manifest
               :pilot-name "Steven Kent"
               :id/rocket "sb002-sol"
               :id/employee "22910x2"
               :badges ["SETUP" "PUT" "DATALOG-QUERIES" "BITEMPORAL" "MATCH"]
               :cargo ["stereo" "gold fish" "slippers" "secret note"]})

  ;; start a temporary, local crux node
(def node (crux/start-node {}))

(comment
  ;; put operation inside a transaction
  (crux/submit-tx node [[:crux.tx/put manifest]])

  ;; read the datum for :manifest
  (crux/entity (crux/db node) :manifest)

  ;; multiple put operations inside a transaction
  (crux/submit-tx node [[:crux.tx/put {:crux.db/id :commodity/Pu}
                         :common-name "Plutonium"
                         :type :element/metal
                         :density 19.816
                         :radioactive true]]

                  [:crux.tx/put {:crux.db/id :commodity/N
                                 :common-name "Nitrogen"
                                 :type :element/gas
                                 :density 1.2506
                                 :radioactive false}]

                  [:crux.tx/put {:crux.db/id :commodity/CH4
                                 :common-name "Methane"
                                 :type :gas
                                 :density 0.717
                                 :radioactive false}])

  (crux/entity (crux/db node) :commodity/Pu)

  ;; using the valid-start-time timestamp 
  (crux/submit-tx node [[:crux.tx/put {:crux.db/id :stock/Pu}
                         :commod :commodity/Pu
                         :weight-ton 21]
                        #inst "2115-02-13T18"]

                  [:crux.tx/put {:crux.db/id :stock/Pu
                                 :commod :commodity/Pu
                                 :weight-ton 23}
                   #inst "2115-02-14T18"]

                  [:crux.tx/put {:crux.db/id :stock/Pu
                                 :commod :commodity/Pu
                                 :weight-ton 22.2}
                   #inst "2115-02-15T18"]

                  [:crux.tx/put {:crux.db/id :stock/Pu
                                 :commod :commodity/Pu
                                 :weight-ton 24}
                   #inst "2115-02-18T18"]

                  [:crux.tx/put {:crux.db/id :stock/Pu
                                 :commod :commodity/Pu
                                 :weight-ton 24.9}
                   #inst "2115-02-19T18"])

  ;; using the timestamp in the query
  (crux/entity (crux/db node #inst "2115-02-14T18") :stock/Pu)
  (crux/entity (crux/db node #inst "2115-02-19T18") :stock/Pu)

  ;; using both the valid-start-time and valid-end-time timestamps
  (crux/submit-tx node [[:crux.tx/put {:crux.db/id :stock/N}
                         :commod :commodity/N
                         :weight-ton 3]
                        #inst "2115-02-13T18"
                        #inst "2115-02-19T18"]

                  [:crux.tx/put {:crux.db/id :stock/CH4
                                 :commod :commodity/CH4
                                 :weight-ton 92}
                   #inst "2115-02-13T18"
                   #inst "2115-02-19T18"])

  ;; a query for these 2 stock documents that is within the start-valid, end-valid range will return a datum
  (crux/entity (crux/db node #inst "2115-02-15") :stock/N)
  (crux/entity (crux/db node #inst "2115-02-13T21") :stock/CH4))

(defmulti ingest (fn [_ doc-content] (vector? doc-content)))

(defmethod ingest true [node docs]
  (crux/submit-tx node (vec (for [doc docs] [:crux.tx/put doc]))))

(defmethod ingest false [node doc]
  (crux/submit-tx node [[:crux.tx/put doc]]))

(comment
  (ingest node [{:crux.db/id :test-doc :from "Steven Kent"}]
          {:crux.db/id :test-doc2 :from "Merle Haggard"}))

;; query basics
(def data [{:crux.db/id :commodity/Pu
            :common-name "Plutonium"
            :type :element/metal
            :density 19.816
            :radioactive true}

           {:crux.db/id :commodity/N
            :common-name "Nitrogen"
            :type :element/gas
            :density 1.2506
            :radioactive false}

           {:crux.db/id :commodity/CH4
            :common-name "Methane"
            :type :molecule/gas
            :density 0.717
            :radioactive false}

           {:crux.db/id :commodity/Au
            :common-name "Gold"
            :type :element/metal
            :density 19.300
            :radioactive false}

           {:crux.db/id :commodity/C
            :common-name "Carbon"
            :type :element/non-metal
            :density 2.267
            :radioactive false}

           {:crux.db/id :commodity/borax
            :common-name "Borax"
            :IUPAC-name "Sodium tetraborate decahydrate"
            :other-names ["Borax decahydrate" "sodium borate" "sodium tetraborate" "disodium tetraborate"]
            :type :mineral/solid
            :appearance "white solid"
            :density 1.73
            :radioactive false}])

(comment
  (crux/q (crux/db node) {:find '[element]
                          :where '[[element :type :element/metal]]})

  (crux/q (crux/db node) {:find '[n d] ; the data to return
                          :where '[[e :type :element/metal] ; get datum that are :element/metal
                                   [e :common-name n] ; bind :common-name to n
                                   [e :density d]]}) ; bind :density to d

  (crux/q (crux/db node) {:find '[name density]
                          :where '[[element :common-name name]
                                   [element :density density]]})

  ;; args cam be used to further filter the results
  (crux/q (crux/db node) {:find '[name]
                          :where '[[e :type t]
                                   [e :common-name name]]
                          :args [{'t :element/metal}]}))

(defn filter-by-id [id crux-node]
  (crux/entity (crux/db crux-node) id))

(defn filter-by-type [type crux-node]
  (crux/q (crux/db crux-node) {:find '[name]
                               :where '[[element :type ?type]
                                        [element :common-name name]]
                               :args [{'?type type}]}))

(defn filter-by-appearance [appearance-description node]
  (crux/q (crux/db node) {:find '[name IUPAC]
                          :where '[[e :common-name name]
                                   [e :IUPAC-name IUPAC]
                                   [e :appearance ?appearance]]
                          :args [{'?appearance appearance-description}]}))

;; valid-start-time, valid-end-time
;; a client's policy history
(comment
  (crux/submit-tx node [[:crux.tx/put {:crux.db/id :consumer/RJ29sUU
                                       :consumer-id :RJ29sUU
                                       :first-name "Jay"
                                       :last-name "Rose"
                                       :cover? true
                                       :cover-type :Promotional}
                         #inst "2111-06-03" ; valid time start
                         #inst "2112-06-03"] ; valid time end

                        [:crux.tx/put {:crux.db/id :consumer/RJ29sUU
                                       :consumer-id :RJ29sUU
                                       :first-name "Jay"
                                       :last-name "Rose"
                                       :cover? false}
                         #inst "2112-06-03"
                         #inst "2112-12-02"]

                        [:crux.tx/put {:crux.db/id :consumer/RJ29sUU
                                       :consumer-id :RJ29sUU
                                       :first-name "Jay"
                                       :last-name "Rose"
                                       :cover? true
                                       :cover-type :Full}
                         #inst "2112-12-03"
                         #inst "2113-12-03"]

                        [:crux.tx/put {:crux.db/id :consumer/RJ29sUU
                                       :consumer-id :RJ29sUU
                                       :first-name "Jay"
                                       :last-name "Rose"
                                       :cover? true
                                       :cover-type :Full}
                         #inst "2113-12-03"
                         #inst "2114-12-03"]])

  ;; period when customer has Full coverage
  (crux/q (crux/db node #inst "2114-01-01") {:find '[covered? coverage-type]
                                             :where '[[customer :consumer-id :RJ29sUU]
                                                      [customer :cover? covered?]
                                                      [customer :cover-type coverage-type]]})
  ;; period when customer has Promotional coverage
  (crux/q (crux/db node #inst "2111-07-03") {:find '[covered? coverage-type]
                                             :where '[[customer :consumer-id :RJ29sUU]
                                                      [customer :cover? covered?]
                                                      [customer :cover-type coverage-type]]})

  ;; period when customer does not have coverage
  (crux/q (crux/db node #inst "2112-08-01") {:find '[covered? coverage-type]
                                             :where '[[customer :consumer-id :RJ29sUU]
                                                      [customer :cover? covered?]
                                                      [customer :cover-type coverage-type]]}))

;; tutorial 5, match transaction
(defn stock-check [company-id item crux-node]
  {:result (crux/q (crux/db crux-node) {:find '[name funds stock]
                                        :where ['[e :company-name name]
                                                '[e :credits funds]
                                                ['e item 'stock]]
                                        :args [{'e company-id}]})
   :item item})

(defn format-stock-check [{:keys [result item]}]
  (for [[name funds stock] result]
    (str "Name: " name ", Funds: " funds ", " item " " stock)))

(comment
  (def stock-data [{:crux.db/id :gold-harmony
                    :company-name "Gold Harmony"
                    :seller? true
                    :buyer? false
                    :units/Au 10211
                    :credits 51}

                   {:crux.db/id :tombaugh-resources
                    :company-name "Tombaugh Resources Ltd."
                    :seller? true
                    :buyer? false
                    :units/Pu 50
                    :units/N 3
                    :units/CH4 92
                    :credits 51}

                   {:crux.db/id :encompass-trade
                    :company-name "Encompass Trade"
                    :seller? true
                    :buyer? true
                    :units/Au 10
                    :units/Pu 5
                    :units/N 3
                    :units/CH4 211
                    :credits 1002}

                   {:crux.db/id :blue-energy
                    :company-name "Blue Energy"
                    :seller? false
                    :buyer? true
                    :credits 1000}])

  (ingest node stock-data)

  ;; task
  ;; check that are enough funds in the buyers account
  ;; before processing a trade
  (crux/submit-tx node [;; blue energy attempting to by 10 units of methane from tombaugh. price 100 units
                        [:crux.tx/match
                         :blue-energy
                         {:crux.db/id :blue-energy
                          :company-name "Blue Energy"
                          :seller? false
                          :buyer? true
                          :credits 1000}]
                        [:crux.tx/put
                         {:crux.db/id :blue-energy
                          :seller? false
                          :buyer? true
                          :company-name "Blue Energy"
                          :credits 900 ; spend 100 credits
                          :units/CH4 10}] ; buy 10 units of methane

                        [:crux.tx/match
                         :tombaugh-resources
                         {:crux.db/id :tombaugh-resources
                          :company-name "Tombaugh Resources Ltd."
                          :seller? true
                          :buyer? false
                          :units/Pu 50
                          :units/N 3
                          :units/CH4 92
                          :credits 51}]
                        [:crux.tx/put
                         {:crux.db/id :tombaugh-resources
                          :company-name "Tombaugh Resources Ltd."
                          :seller? true
                          :buyer? false
                          :units/Pu 50
                          :units/N 3
                          :units/CH4 82 ; sell 10 units of methane
                          :credits 151}] ; receive 100 credits
                        ])

  (format-stock-check (stock-check :tombaugh-resources :units/CH4 node))
  (format-stock-check (stock-check :blue-energy :units/CH4 node))

;; example of not enough credits in the buyer's account
  ;; encompass trade attempt to purchase 10_000 units of gold from gold harmony. price (tutorial doesn't state) credits
  (crux/submit-tx node
                  [[:crux.tx/match
                    :gold-harmony
                    {:crux.db/id :gold-harmony
                     :company-name "Gold Harmony"
                     :seller? true
                     :buyer? false
                     :units/Au 10211
                     :credits 51}]
                   [:crux.tx/put
                    {:crux.db/id :gold-harmony
                     :company-name "Gold Harmony"
                     :seller? true
                     :buyer? false
                     :units/Au 211
                     :credits 10051}]

                   [:crux.tx/match
                    :encompass-trade
                    {:crux.db/id :encompass-trade
                     :company-name "Encompass Trade"
                     :seller? true
                     :buyer? true
                     :units/Au 10
                     :units/Pu 5
                     :units/CH4 211
                     :credits 10000}] ;; FAILS HERE. encompass does not have this many credits
                   [:crux.tx/put
                    {:crux.db/id :encompass-trade
                     :company-name "Encompass Trade"
                     :seller? true
                     :buyer? true
                     :units/Au 10010
                     :units/Pu 5
                     :units/CH4 211
                     :credits 1002}]])

;; check that transaction did not complete
  (format-stock-check (stock-check :gold-harmony :units/Au node))
  (format-stock-check (stock-check :encompass-trade :units/Au node))

;; check to see if you still have the secret note
  (crux/q (crux/db node) {:find '[belongings]
                          :where '[[e :cargo belongings]]
                          :args [{'belongings "secret note"}]}))

;; tutorial 6 delete transaction
(comment
  (crux/submit-tx node
                  [[:crux.tx/put {:crux.db/id :kaarlang/clients
                                  :clients [:encompass-trade]}
                    #inst "2110-01-01T09"
                    #inst "2111-01-01T09"]

                   [:crux.tx/put {:crux.db/id :kaarlang/clients
                                  :clients [:encompass-trade :blue-energy]}
                    #inst "2111-01-01T09"
                    #inst "2113-01-01T09"]

                   [:crux.tx/put {:crux.db/id :kaarlang/clients
                                  :clients [:blue-energy]}
                    #inst "2113-01-01T09"
                    #inst "2114-01-01T09"]

                   [:crux.tx/put {:crux.db/id :kaarlang/clients
                                  :clients [:blue-energy :gold-harmony :tombaugh-resources]}
                    #inst "2114-01-01T09"
                    #inst "2115-01-01T09"]])

  ;; delete entire document history.
  (crux/submit-tx node [[:crux.tx/delete
                         :kaarlang/clients
                         #inst "2110-01-01"
                         #inst "2116-01-01"]]))

;; using pull
(defn full-query [node]
  (crux/q 
   (crux/db node) 
   '{:find [(pull ?id [*])]
     :where [[?document :crux.db/id ?id]]}))

(defn full-query-one-entity [node eid]
  (crux/q
   (crux/db node)
   {:find '[(pull e [*])]
     :where '[[e :crux.db/id id]]
     :args [{'id eid}]}))

(defn pull-one-person [node eid]
  (crux/pull
   (crux/db node)
   [:crux.db/id :full-name]
   eid))

(defn pull-many-people [node eids]
  (crux/pull-many
   (crux/db node)
   [:crux.db/id :full-name]
   eids))

;; tutorial 7 evict transaction
(comment
  (crux/submit-tx node [[:crux.tx/put
                         {:crux.db/id :person/kaarlang
                          :full-name "Kaarlang"
                          :origin-planet "Mars"
                          :identity-tag :KA01299242093
                          :DOB #inst "2040-11-23"}]

                        [:crux.tx/put
                         {:crux.db/id :person/ilex
                          :full-name "Ilex Jefferson"
                          :origin-planet "Venus"
                          :identity-tag :IJ01222212454
                          :DOB #inst "2061-02-17"}]

                        [:crux.tx/put
                         {:crux.db/id :person/thadd
                          :full-name "Thad Christover"
                          :origin-moon "Titan"
                          :identity-tag :IJ01222212454
                          :DOB #inst "2101-01-01"}]

                        [:crux.tx/put
                         {:crux.db/id :person/johanna
                          :full-name "Johanna"
                          :origin-planet "Earth"
                          :identity-tag :JA012992129120
                          :DOB #inst "2090-12-07"}]])

  ;; evict transaction
  (crux/submit-tx node [[:crux.tx/evict :person/kaarlang]])
  )

;; tutorial 8, await transactions with await-tx
(comment
  (def stats
    [{:body "Sun"
      :type "Star"
      :units {:radius "Earth Radius"
              :volume "Earth Volume"
              :mass "Earth Mass"
              :gravity "Standard gravity (g)"}
      :radius 109.3
      :volume 1305700
      :mass 33000
      :gravity 27.9
      :crux.db/id :Sun}
     
     {:body "Jupiter"
      :type "Gas Giant"
      :units {:radius "Earth Radius"
              :volume "Earth Volume"
              :mass "Earth Mass"
              :gravity "Standard gravity (g)"}
      :radius 10.97
      :volume 1321
      :mass 317.83
      :gravity 2.52
      :crux.db/id :Jupiter}
     
     {:body "Saturn"
      :units {:radius "Earth Radius"
              :volume "Earth Volume"
              :mass "Earth Mass"
              :gravity "Standard gravity (g)"}
      :radius 9.14
      :volume 764
      :mass 95.162
      :gravity 1.065
      :type "planet"
      :crux.db/id :Saturn}
     
     {:body "Uranus"
      :units {:radius "Earth Radius"
              :volume "Earth Volume"
              :mass "Earth Mass"
              :gravity "Standard gravity (g)"}
      :radius 3.981
      :volume 63.1
      :mass 14.536
      :gravity 0.886
      :type "planet"
      :crux.db/id :Uranus}
     
     {:body "Neptune"
      :units {:radius "Earth Radius"
              :volume "Earth Volume"
              :mass "Earth Mass"
              :gravity "Standard gravity (g)"}
      :radius 3.865
      :volume 57.7
      :mass 17.147
      :gravity 1.137
      :type "planet"
      :crux.db/id :Neptune}
     
     {:body "Earth"
      :units {:radius "Earth Radius"
              :volume "Earth Volume"
              :mass "Earth Mass"
              :gravity "Standard gravity (g)"}
      :radius 1
      :volume 1
      :mass 1
      :gravity 1
      :type "planet"
      :crux.db/id :Earth}
     
     {:body "Venus"
      :units {:radius "Earth Radius"
              :volume "Earth Volume"
              :mass "Earth Mass"
              :gravity "Standard gravity (g)"}
      :radius 0.9499
      :volume 0.857
      :mass 0.815
      :gravity 0.905
      :type "planet"
      :crux.db/id :Venus}
     
     {:body "Mars"
      :units {:radius "Earth Radius"
              :volume "Earth Volume"
              :mass "Earth Mass"
              :gravity "Standard gravity (g)"}
      :radius 0.532
      :volume 0.151
      :mass 0.107
      :gravity 0.379
      :type "planet"
      :crux.db/id :Mars}
     
     {:body "Ganymede"
      :units {:radius "Earth Radius"
              :volume "Earth Volume"
              :mass "Earth Mass"
              :gravity "Standard gravity (g)"}
      :radius 0.4135
      :volume 0.0704
      :mass 0.0248
      :gravity 0.146
      :type "moon"
      :crux.db/id :Ganymede}
     
     {:body "Titan"
      :units {:radius "Earth Radius"
              :volume "Earth Volume"
              :mass "Earth Mass"
              :gravity "Standard gravity (g)"}
      :radius 0.4037
      :volume 0.0658
      :mass 0.0225
      :gravity 0.138
      :type "moon"
      :crux.db/id :Titan}
     
     {:body "Mercury"
      :units {:radius "Earth Radius"
              :volume "Earth Volume"
              :mass "Earth Mass"
              :gravity "Standard gravity (g)"}
      :radius 0.3829
      :volume 0.0562
      :mass 0.0553
      :gravity 0.377
      :type "planet"
      :crux.db/id :Mercury}])

(def kepra-5 {:body "Kepra-5"
              :units {:radius "Earth Radius"
                      :volume "Earth Volume"
                      :mass "Earth mass"
                      :gravity "Standard gravity (g)"}
              :radius 0.6729
              :volume 0.4562
              :mass 0.5653
              :gravity 1.4
              :type "planet"
              :crux.db/id :Kepra-5})

(ingest node kepra-5)

;; determine the rank of kepra-5's gravitational force
(->> (crux/q
      (crux/db node)
      {:find '[planet g]
       :where '[[planet :gravity g]]})
     (sort-by #(last %)))

;; use await-tx
(crux/await-tx 
 node
 (crux/submit-tx node [[:crux.tx/put kepra-5]]))

(def test-traveler {:crux.db/id :origin-planet/test-traveler
                    :chosen-name "Test"
                    :given-name "Test Traveler"
                    :passport-number (java.util.UUID/randomUUID)
                    :stamps []
                    :penalties []})

(def test-traveler2 {:crux.db/id :origin-planet/new-test-traveler
                     :chosen-name "Testy"
                     :given-name "Test Traveler"
                     :passport-number (java.util.UUID/randomUUID)
                     :stamps []
                     :penalties []})

;; application of await-tx
(defn ingest-and-query
  "Ingests the travelers document, returns the passport number once the transaction is complete"
  [traveler-doc node]
  (crux/await-tx node 
                 (crux/submit-tx node [[:crux.tx/put traveler-doc]]))
  (crux/q
   (crux/db node)
   {:find '[?p-n]
    :where '[[traveler :crux.db/id id]
             [traveler :passport-number ?p-n]]
    :args [{'id (:crux.db/id traveler-doc)}]}))








)




(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
