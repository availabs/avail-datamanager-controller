import { Context } from "moleculer";
import { FSA } from "flux-standard-action";

export type DamaSource = {
  source_id: numberl;
  name: string;
  update_interval?: string;
  category?: string;
  description?: string;
  statistics?: any;
  metadata?: any;
  categories?: any;
  type?: string;
  
  user_id?: number;

  _created_timestamp?: Date;
  _modified_timestamp?: Date;
};

export type DamaView = {
  view_id: number;

  source_id: number;

  data_type?: string;
  interval_version?: string;
  geography_version?: string;
  version?: string;
  source_url?: string;
  publisher?: string;
  table_schema?: string;
  table_name?: string;
  download_url?: string;
  tiles_url?: string;
  start_date?: Date;
  end_date?: Date;
  last_updated?: Date;
  statistics?: any;
  metadata?: any;

  user_id?: number;

  root_etl_context_id?: number;
  etl_context_id?: number;

  _created_timestamp?: Date;
  _modified_timestamp?: Date;
};

export type TransactionContext = Context & {
  params: {
    events: FSA[];
    eventsByType: Record<FSA["type"], FSA[]>;
    newDamaSource: DamaSource | null;
    newDamaView: DamaView | null;
  };
  meta: { transactionId: string };
};
