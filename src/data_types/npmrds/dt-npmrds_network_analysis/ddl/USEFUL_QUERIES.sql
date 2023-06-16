select distinct
	node_id,
	wkb_geometry
from npmrds_network_spatial_analysis.npmrds_network_nodes_2021 AS a
	inner join npmrds_network_spatial_analysis.level_1_conformal_matches(
	  'npmrds_network_spatial_analysis.npmrds_network_node_incident_edges_metadata_2021',
	  'npmrds_network_spatial_analysis.npmrds_network_node_incident_edges_metadata_2022'
	) AS b ON ( a.node_id = b.node_id_a )
;


select distinct
	node_id,
	wkb_geometry
from npmrds_network_spatial_analysis.npmrds_network_node_incident_edges_metadata_2021 AS a
	left outer join npmrds_network_spatial_analysis.level_1_conformal_matches(
	  'npmrds_network_spatial_analysis.npmrds_network_node_incident_edges_metadata_2021',
	  'npmrds_network_spatial_analysis.npmrds_network_node_incident_edges_metadata_2022'
	) AS b ON ( a.node_id = b.node_id_a )
where b.node_id_a is null
;
