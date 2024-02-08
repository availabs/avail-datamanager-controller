// FIXME: These need to eventually be accessible from the data_manager.views table.
// FIXME: Until DaMa integration, could use
//
// npmrds_production=# \d conflation_map_versions_meta
//       View "conflation.conflation_map_versions_meta"
//     Column     |  Type   | Collation | Nullable | Default
// ---------------+---------+-----------+----------+---------
//  year          | integer |           |          |
//  major_version | integer |           |          |
//  minor_version | integer |           |          |
//  patch_version | integer |           |          |

export const conflation_version = "v0_6_0";

export const min_year = 2016;
export const max_year = 2024;
