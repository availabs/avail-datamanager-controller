export type DamaSourceName = string;

export type DataSourceInitialMetadata = {
  name: DamaSourceName;
  update_interval?: string | null;
  category?: string[] | null;
  description: string;
  categories?: string[] | null;
  type: string;
  display_name: string;
  source_dependencies_names?: string[] | null;
  metadata?: object & {
    elements?: Record<DamaSourceName, DataSourceInitialMetadata>;
  };
};

export type DamaSource = {
  source_id: number;
  name: DamaSourceName;
  update_interval: string | null;
  category: string[] | null;
  description: string | null;
  statistics: object | null;
  metadata: object | null;
  categories: string[] | null;
  type: string;
  display_name: string | null;
  source_dependencies: number[] | null;
  user_id: number;
  _created_timestamp: Date;
  _modified_timestamp: Date;
};

export type DamaView = {
  view_id: number;
  source_id: number;
  data_type?: string | null;
  interval_version?: string | null;
  geography_version?: string | null;
  version?: string | null;
  source_url?: string | null;
  publisher?: string | null;
  table_schema?: string | null;
  table_name?: string | null;
  data_table?: string | null;
  download_url?: string | null;
  tiles_url?: string | null;
  start_date?: Date | null;
  end_date?: Date | null;
  last_updated?: Date | null;
  statistics?: object | null;
  metadata?: object | null;
  user_id?: number | null;
  etl_context_id?: number | null;
  view_dependencies?: number[] | null;
  active_start_timestamp?: Date;
  active_end_timestamp?: Date;
  _created_timestamp?: Date;
  _modified_timestamp?: Date;
};
