(ns metabase.api.dataset
  "/api/dataset endpoints."
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [compojure.core :refer [POST]]
            [metabase
             [query-processor :as qp]
             [util :as u]]
            [metabase.api.common :as api]
            [metabase.models
             [database :refer [Database]]
             [query :as query]]
            [metabase.query-processor.util :as qputil]
            [metabase.util
             [format :as fmt]
             [schema :as su]]
            [schema.core :as s])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

(def ^:private ^:const max-results-bare-rows
  "Maximum number of rows to return specifically on :rows type queries via the API."
  2000)

(def ^:private ^:const max-results
  "General maximum number of rows to return from an API query."
  10000)

(def ^:const default-query-constraints
  "Default map of constraints that we apply on dataset queries executed by the api."
  {:max-results           max-results
   :max-results-bare-rows max-results-bare-rows})

(api/defendpoint POST "/"
  "Execute a query and retrieve the results in the usual format."
  [:as {{:keys [database] :as body} :body}]
  (api/read-check Database database)
  ;; add sensible constraints for results limits on our query
  (let [query (assoc body :constraints default-query-constraints)]
    (qp/dataset-query query {:executed-by api/*current-user-id*, :context :ad-hoc})))

;; TODO - this is no longer used. Should we remove it?
(api/defendpoint POST "/duration"
  "Get historical query execution duration."
  [:as {{:keys [database], :as query} :body}]
  (api/read-check Database database)
  ;; try calculating the average for the query as it was given to us, otherwise with the default constraints if there's no data there.
  ;; if we still can't find relevant info, just default to 0
  {:average (or (query/average-execution-time-ms (qputil/query-hash query))
                (query/average-execution-time-ms (qputil/query-hash (assoc query :constraints default-query-constraints)))
                0)})

(def ExportFormat
  "Schema for valid export formats for downloading query results."
  fmt/ExportFormat)

(defn export-format->context
  "Return the `:context` that should be used when saving a QueryExecution triggered by a request to download results in EXPORT-FORAMT.

     (export-format->context :json) ;-> :json-download"
  [export-format]
  (or (get-in fmt/export-formats [export-format :context])
      (throw (Exception. (str "Invalid export format: " export-format)))))

(defn as-format
  "Return a response containing the RESULTS of a query in the specified format."
  {:style/indent 1, :arglists '([export-format results])}
  [export-format {:keys [status], :as response}]
  (api/let-404 [export-result (fmt/as-format export-format response)]
    (if (= status :completed)
      ;; successful query, send file
      {:status  200
       :body    (:body export-result)
       :headers {"Content-Type"        (str (:content-type export-result) "; charset=utf-8")
                 "Content-Disposition" (str "attachment; filename=\"query_result_" (u/date->iso-8601) "." (:ext export-result) "\"")}}
      ;; failed query, send error message
      {:status 500
       :body   (:error response)})))

(def export-format-regex
  "Regex for matching valid export formats (e.g., `json`) for queries.
   Inteneded for use in an endpoint definition:

     (api/defendpoint POST [\"/:export-format\", :export-format export-format-regex]"
  (re-pattern (str "(" (str/join "|" (keys fmt/export-formats)) ")")))

(api/defendpoint POST ["/:export-format", :export-format export-format-regex]
  "Execute a query and download the result data as a file in the specified format."
  [export-format query]
  {query         su/JSONString
   export-format ExportFormat}
  (let [query (json/parse-string query keyword)]
    (api/read-check Database (:database query))
    (as-format export-format
      (qp/dataset-query (dissoc query :constraints)
        {:executed-by api/*current-user-id*, :context (export-format->context export-format)}))))


(api/define-routes)
