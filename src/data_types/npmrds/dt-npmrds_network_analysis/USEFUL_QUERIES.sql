-- All reference nodes
select
        a.*,
        st_makeline(
                b.wkb_geometry,
                c.wkb_geometry
        ) as wkb_geometry
from npmrds_network_spatial_analysis.npmrds_network_conformal_matches_2012_2022 as a
        inner join npmrds_network_spatial_analysis.npmrds_network_nodes_2017 as b
                on (a.node_id_a = b.node_id)
        inner join npmrds_network_spatial_analysis.npmrds_network_nodes_2022 as c
                on (a.node_id_b = c.node_id)
where ( COALESCE(a.match_class, '') != 'TMC_LINEAR_PATHS_OVERLAP_INTERNAL_NODES' )
