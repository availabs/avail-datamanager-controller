# dt-transcom_events

## FIXME

* Hard-coded date in ./constants/conflation_map_meta.ts filters which events
  get snapped to the conflation map.
* Only the currently downloaded events are snapped if the events pass through
  the above mentioned filter.
* Therefore, after the hard-coded years filter is changed, any downloaded
  events that did not initially pass the filter do not get snapped to the map.
* Easiest thing to do is re-download them.

Case in point, the max year was not updated to 2024 until Feb 6th.
The January events were not snapped to the map after the change.
Needed to re-download the January events.

## TODO

- [ ] Document the TRANSCOM ETL process

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
