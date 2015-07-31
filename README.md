# Elasticsearch Stress Tool

Elasticsearch Stress Tool is a tool to check Elasticsearch configuration, the underlying hardware.
It can be used to compare hardware or cloud solutions.
It's inspired by Cassandra Stress Tool.
It's not a full bleed benchmarking tool, you'd be better user JMeter, Gatling, Loader.io to build
real benchmarks.

## Features

* Generate documents/queries from mixing data from a CSV file with document/queries based on Mustache
* Index many documents in an index or query an index with varying queries
* Iterate many times with many threads
* Measure execution times

Indexing documents

```
esstresstool index -h localhost:9300 -c my_cluster \ 
  -di my_index -dt my_type \
  -dd doc_data.csv -dm doc_template.mustache \
  -t 4 -n 1000000
```

Querying documents

```
esstresstool search -h localhost:9300 -c my_cluster \ 
  -di my_index -dt my_type \
  -qd query_data.csv -qm query_template.mustache \
  -t 4 -n 1000000
```

## Options

| Option   | Default  | Description |
|----------|----------|-------------|
| -h, --host  | localhost:9300  | Hosts and ports of the Elasticsearch cluster |
| -c, --cluster  |   | Cluster name  |
| -x, --protocol | transport  | How to connect to Elasticsearch cluster: `transport`, `node`  |
| -b, --bulk-size | 1  | Use a value greater than one to do bulk insert |
| -di, --doc-index, --index  | `.esstresstool`  | Target index or indices  |
| -dt, --doc-type  |   | Document type or types  |
| -n, --iterations  |    | Number of iterations for each thread  |
| -t, --thread  | Number of processors| Number of threads  |
| -dd, --doc-data, -qd, --query-data |   | CSV file used to inject data. Can be omitted to have constant data. `names` value gives gives the liste of Marvel super heroes  |
| -dm, --doc-template, -qm, --query-template  |     | A Mustache template used to generate Documents or Queries. Can be omitted to use data has documents. |
|  -m, --metric-period  |  10  | Period in second for metric reporting  | 

## Files

### CSV Data File

A comma separated file with headers is expected. 
The header are used as field names and can be references in Mustache templates.
This file will be read multiple times to get the expected iteration number.

There are some special column names:

| Name   |  Description |
|--------|--------------|
| `index`  | Target index, overrides command line `-di` option  |
| `type`  | Document type, overrides command line `-dd` option  |
| `id`  | Document Id  |

### Mustache Template File

You can use in the Document/Query template any field provided.

There are some special fields:

| Name   |  Description |
|--------|--------------|
| `docIndex`  | Target index |
| `docType`  | Document type |
| `docId`  | Document Id, if read from CSV |
| `docNumber`  | Document index between 0 and thread&times;iterations&times;bulk size |
| `timestamp`  | Timestamp in milliseconds |
| `random.boolean`  | Random boolean |
| `random.int`  | Random integer |
| `random.long`  | Random long |
| `random.float`  | Random float |
| `random.double`  | Random double |
