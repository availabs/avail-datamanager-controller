#!../../node_modules/.bin/ts-node -T

import { createReadStream } from "fs";
import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import pgFormat from "pg-format";

import fetch from "cross-fetch";
import FormData from "form-data";

const damaUrlBase = "http://127.0.0.1:3369/dama-admin/dama_dev_1";

const inputFile = join(__dirname, "lines.zip");
const tableDescriptorPath = join(__dirname, "test_post_table_descriptor.json");

async function uploadGisFile() {
  const form = new FormData();
  form.append("upload", createReadStream(inputFile));

  const resp = await fetch(
    `${damaUrlBase}/staged-geospatial-dataset/uploadGeospatialDataset`,
    // @ts-ignore
    { method: "POST", body: form }
  );

  const [{ id }] = await resp.json();

  return id;
}

async function getLayerNames(uploadId: string) {
  const resp = await fetch(
    `${damaUrlBase}/staged-geospatial-dataset/${uploadId}/layerNames`
  );

  return resp.json();
}

async function updateTableDescriptor(uploadId: string) {
  const tableDescriptor = await readFileAsync(tableDescriptorPath, {
    encoding: "utf8",
  });

  await fetch(
    `${damaUrlBase}/staged-geospatial-dataset/${uploadId}/updateTableDescriptor`,
    {
      method: "POST",
      body: tableDescriptor,
      headers: {
        "Content-Type": "application/json",
      },
    }
  );
}

async function loadDatabaseTable(uploadId: string, layerName: string) {
  const event = {
    type: "dama/data_source_integrator:LOAD_REQUEST",
    payload: { id: uploadId, layerName },
    meta: { DAMAA: true },
  };

  const resp = await fetch(`${damaUrlBase}/events/dispatch`, {
    method: "POST",
    body: JSON.stringify(event),
    headers: {
      "Content-Type": "application/json",
    },
  });

  //  NOTE: Loading returns an etl_context_id.
  //        From here on, we use the etl_context_id rather than the uploadId.
  const {
    meta: { etl_context_id },
  } = await resp.json();

  return etl_context_id;
}

async function approveQA(etl_context_id: number) {
  const event = {
    type: "dama/data_source_integrator:QA_APPROVED",
    meta: {
      DAMAA: true,
      etl_context_id,
      timestamp: new Date().toISOString(),
    },
  };

  await fetch(`${damaUrlBase}/events/dispatch`, {
    method: "POST",
    body: JSON.stringify(event),
    headers: {
      "Content-Type": "application/json",
    },
  });
}

async function submitViewMeta(etl_context_id: number) {
  const tstamp = new Date()
    .toISOString()
    .replace(/[^0-9tz]/gi, "")
    .toLowerCase();

  const table_schema = "dama_gis";
  const table_name = `lines_test_${tstamp}`;

  const viewMeta = {
    data_source_name: "dama_gis_integration_test",
    table_schema,
    table_name,
    data_table: pgFormat("%I.%I", table_schema, table_name),
  };

  const event = {
    type: "dama/data_source_integrator:VIEW_METADATA_SUBMITTED",
    payload: viewMeta,
    meta: {
      DAMAA: true,
      etl_context_id,
      timestamp: new Date().toISOString(),
    },
  };

  await fetch(`${damaUrlBase}/events/dispatch`, {
    method: "POST",
    body: JSON.stringify(event),
    headers: {
      "Content-Type": "application/json",
    },
  });
}

async function publish(etl_context_id: number) {
  const event = {
    type: "dama/data_source_integrator:PUBLISH",
    meta: {
      DAMAA: true,
      etl_context_id,
      timestamp: new Date().toISOString(),
    },
  };

  await fetch(`${damaUrlBase}/events/dispatch`, {
    method: "POST",
    body: JSON.stringify(event),
    headers: {
      "Content-Type": "application/json",
    },
  });
}

async function main() {
  const uploadId = await uploadGisFile();

  const [layerName] = await getLayerNames(uploadId);

  await updateTableDescriptor(uploadId);

  const etl_context_id = await loadDatabaseTable(uploadId, layerName);

  while (true) {
    const resp = await fetch(
      `${damaUrlBase}/events/query?etl_context_id=${etl_context_id}`
    );

    const events = await resp.json();

    if (events.some(({ type }) => /:STAGED$/.test(type))) {
      break;
    }

    await new Promise((resolve) => setInterval(resolve, 1000));
  }

  await Promise.all([
    approveQA(etl_context_id),
    submitViewMeta(etl_context_id),
  ]);

  while (true) {
    const resp = await fetch(
      `${damaUrlBase}/events/query?etl_context_id=${etl_context_id}`
    );

    const events = await resp.json();

    if (events.some(({ type }) => /:READY_TO_PUBLISH/.test(type))) {
      break;
    }

    await new Promise((resolve) => setInterval(resolve, 1000));
  }

  await publish(etl_context_id);

  console.log("DONE");

  // FIXME: No idea why this script hangs.
  process.exit();
}

main();
