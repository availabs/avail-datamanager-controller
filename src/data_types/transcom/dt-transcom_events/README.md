# dt-transcom_events

## TODO

- [ ] Handle case where conflation version changes.

  - Right now, conflation_version is in ./constants/conflation_map_meta.ts
  - The ETL code does not handle loading new conflation version tables from scratch.
  - Make conflation_version a data_manager.views property
    - If previous DamaView's conflation_version != cur conflation_version, redo
      - transcom.transcom_events_onto_conflation_map
      - transcom.transcom_events_onto_road_network
      - transcom.transcom_events_by_tmc_summary

- [ ] Reduce time tables/views are LOCKED

  - [ ] Change transcom.transcom_events_onto_road_network from MATERIALIZED VIEW to TABLE
    - [ ] DELETE/INSERT only the newly downloaded events
    - [ ] CLUSTER only if nightly ETL


  - [ ] Change transcom.transcom_events_by_tmc_summary from MATERIALIZED VIEW to TABLE
    - [ ] DELETE/INSERT only the newly downloaded events
    - [ ] CLUSTER only if nightly ETL
