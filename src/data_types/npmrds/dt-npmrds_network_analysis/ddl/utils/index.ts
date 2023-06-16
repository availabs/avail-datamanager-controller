export const network_spatial_analysis_schema_name =
  "npmrds_network_spatial_analysis";

export const temporary_tmc_points_table_name = "tmp_tmc_points";

export const coord_rounding_precision = 6;

export const bearing_distance = 20; // meters

export function getTmcShapesTableInfo(year: number) {
  return {
    table_schema: network_spatial_analysis_schema_name,
    table_name: `tmc_shapes_${year}`,
  };
}

export function getNpmrdsNetworkNodesTableInfo(year: number) {
  const table_name = `npmrds_network_nodes_${year}`;

  return {
    table_schema: network_spatial_analysis_schema_name,
    table_name,
    pkey_idx_name: `${table_name}_pkey`,
    coord_idx_name: `${table_name}_coord_idx`,
  };
}

export function getNpmrdsNetworkEdgesTableInfo(year: number) {
  const table_name = `npmrds_network_edges_${year}`;

  return {
    table_schema: network_spatial_analysis_schema_name,
    table_name,
    pkey_idx_name: `${table_name}_pkey`,
    node_idx_name: `${table_name}_node_id_idx`,
  };
}

export function getNpmrdsNetworkEdgesMetadataViewInfo(year: number) {
  const table_name = `npmrds_network_edge_metadata_${year}`;

  return {
    table_schema: network_spatial_analysis_schema_name,
    table_name,
    pkey_idx_name: `${table_name}_pkey`,
    node_idx_name: `${table_name}_node_id_idx`,
  };
}

export function getNpmrdsNetworkEdgeMaxGeomIdxTableInfo(year: number) {
  const table_name = `npmrds_network_edges_max_geom_idx_${year}`;

  return {
    table_schema: network_spatial_analysis_schema_name,
    table_name,
    pkey_idx_name: `${table_name}_pkey`,
  };
}

export function getNpmrdsNetworkPathsTableInfo(year: number) {
  const table_name = `npmrds_network_paths_${year}`;

  return {
    table_schema: network_spatial_analysis_schema_name,
    table_name,
    pkey_idx_name: `${table_name}_pkey`,
  };
}

export function getNpmrdsNetworkNodeIncidentEdgesInfo(year: number) {
  const table_name = `npmrds_network_node_incident_edges_${year}`;

  return {
    table_schema: network_spatial_analysis_schema_name,
    table_name,
    pkey_idx_name: `${table_name}_pkey`,
  };
}

export function getNpmrdsNetworkNodeIncidentEdgesMetadataInfo(year: number) {
  const table_name = `npmrds_network_node_incident_edges_metadata_${year}`;

  return {
    table_schema: network_spatial_analysis_schema_name,
    table_name,
  };
}

export function getNpmrdsNetworkNodeLevelLabelsInfo(
  year: number,
  level: number
) {
  const table_name = `npmrds_network_node_level_${level}_labels_${year}`;

  return {
    table_schema: network_spatial_analysis_schema_name,
    table_name,
    pkey_idx_name: `${table_name}_pkey`,
  };
}
