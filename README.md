# Elasticsearch Stress Tool

Elasticsearch Stress Tool is a tool to check Elasticsearch configuration, the underlying hardware.

It can be used to compare:

* hardware or cloud solutions,
* sharding or replication settings,
* mappings and analyzers,
* HTTP and Transport protocols,
* different Elasticsearch versions.

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

### Common options

| Option   | Description | Default |
|----------|----------|-------------|
| `--help`  | Print help | |
| `-x`, `--protocol` | Protocol, either  `node`, `transport`, `jest`, `http` | `transport` |
| `-h`, `--host` | Hosts and ports | `localhost` port depends on transport |
| `-c`, `--cluster` | Cluster name | |
| `-t`, `--thread` | Thread number | Number of CPUs  |
| `-sp`, `--start-period-ms` | Period in ms between each thread start |  |
| `-ep`, `--execute-period-ms` | Period in ms between each execution | |
| `-n`, `--iterations` | Number of iterations of each thread | 10000 |
| `-di`, `--doc-index`, `--index` | Default Index name | `.stresstest` |
| `-dt`, `--doc-type` | Default Document type | stress |
| `-dd`, `--doc-data`, `-qd`, `--query-data` | Document/Query data CSV file |  |
| `-dm`, `--doc-template`, `-qm`, `--query-template` | Document/Query Mustache file |  |
| `-mc`, `--metric-console` | Output Console for metric reporting | `false` |
| `-mo`, `--metric-output` | Output File for metric reporting, ending either `.csv` or .json | |
| `-mp`, `--metric-period-ms` | Period in second for metric reporting | 10000 |

### Index options

| Option   | Description | Default |
|----------|----------|-------------|
| `-b`, `--bulk-size` | Bulk size, 1 to disable bulk | |
| `-did`, `--doc-index-delete` | Index delete at startup | `false` |
| `-dis`, `--doc-index-settings`, `--index-settings` | Index settings for creation at startup | |

Before indexing data, you can delete the index and/or create the index with specified settings.

### Search options

Nothing at the moment

## Files

### Data File: CSV

This file is used to produce data which can be indexed or used as query parameters.
It will be read many times if the number of iterations is bigger than the number of lines.

A comma separated (CSV) file with headers is expected.
The header are used as field names and can be referenced in Mustache templates.
This file will be read multiple times to get the expected iteration number.

There are some special column names:

| Name   |  Description |
|--------|--------------|
| `index`  | Target index, overrides command line `-di` option  |
| `type`  | Document type, overrides command line `-dt` option  |
| `id`  | Document Id  |

There is also a special Data File called `names` which references Marvel super heroes.

### Template File: Mustache

This file is used to convert data in JSON: the document to index or the query to execute.
If no template is specified, then the document to index is built from data using CSV headers.

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

### Metrics file: CSV or JSON

Response times are measured using Yammer/Codahale/Dropwizard Metrics library.
Then they can be periodically written to logs, to a CSV or JSON file.
The JSON file format can easily be imported into Elasticsearch with Logstash and used to build charts with Kibana.