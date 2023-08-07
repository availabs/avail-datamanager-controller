export enum LayerName {
  WATERBODY = "NHDWaterbody",
}

export const waterbody_dama_source_info = {
  name: "usgs_nhd_waterbody",
  display_name: "USGS National Hydrography Dataset (Waterbody)",
};

export const waterbody_dama_view_info = {
  table_schema: "usgs_national_hydrography_dataset",
  table_name: "waterbody",
  geography_version: 36,
  source_url:
    "https://www.sciencebase.gov/catalog/item/61f8b8e1d34e622189c32924",
  publisher: "USGS",
};
