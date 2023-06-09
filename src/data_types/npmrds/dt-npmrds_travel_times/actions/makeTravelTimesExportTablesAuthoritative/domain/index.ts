import {
  NodePgClient,
  NodePgPoolClient,
} from "../../../../../../data_manager/dama_db/postgres/PostgreSQL";

export type NodePgDbConnection = NodePgClient | NodePgPoolClient;

export type ParsedNpmrdsTravelTimesExportTableMetadata = {
  damaViewId: number;

  table_schema: string;
  table_name: string;

  lastUpdated: string;

  state: string;
  year: number;
  month: number;

  is_complete_week: boolean;
  is_complete_month: boolean;
  is_expanded: boolean;

  // [ startDate, endDate ]
  start_date: string;
  end_date: string;
};

export type EttViewsMetaSummary = {
  byViewId: Record<
    ParsedNpmrdsTravelTimesExportTableMetadata["damaViewId"],
    ParsedNpmrdsTravelTimesExportTableMetadata
  >;

  sortedByStateThenStartDate: ParsedNpmrdsTravelTimesExportTableMetadata["damaViewId"][];

  byMonthByYearByState: Record<
    ParsedNpmrdsTravelTimesExportTableMetadata["state"],
    Record<
      ParsedNpmrdsTravelTimesExportTableMetadata["year"],
      Record<ParsedNpmrdsTravelTimesExportTableMetadata["month"], number[]>
    >
  >;

  dataDateRange: [string, string];

  lastUpdated: string;
};
