# Data Manager Backend

**NOTE: This code is in the process of being merged into the avail-datamanager-controller core.**

## Installing Dependencies

This directory contains its own _package.json_ file to separate its
dependencies from the overall avail-falcor project's.

```sh
npm install
```

## Usage

### The staged-geospatial-dataset-tileserver

NOTE: The staged-geospatial-dataset-tileserver is currently hardcoded to use
port 5960.

```sh
./staged-geospatial-dataset-tileserver/start
```

### The dataset-integration-server

NOTE: The dataset is currently hardcoded to use
port 5566.

NOTE: The dataset-integration-server is currently hardwired
to use a development database. The development database configuration
should be in _../db_service/npmrds.config.local.json_.

```json
{
    "host": "127.0.0.1",
    "port": 5432,
    "user": "foo",
    "password": "bar",
    "database": "baz"
}
```

```sh
./dataset-integration-server/start
```

You will be able to upload datasources via the UI on port 5566.

### Integrating a Geospatial Dataset

You can use the UI to load a GIS datasource.

Additionally, the `integrateNewGeospatialDataset` function in
[dataset-integration-server/tasks.ts](./dataset-integration-server/tasks.ts)
shows the series of _dataset-integration-server_ API calls
used to integrate a new Geospatial Dataset.

To run the tasks module,

```sh
./node_modules/.bin/ts-node dataset-integration-server/tasks.ts
```
