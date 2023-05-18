(defproject tique-checker "0.1.0-SNAPSHOT"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [elle "0.1.4"]
                 [spootnik/unilog "0.7.29"]
                 [cheshire "5.10.2"]
                 ]
  :main tique-checker.main
  :repl-options {:init-ns tique-checker.core})
