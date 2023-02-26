import {
  NodePgClient,
  NodePgPoolClient,
} from "../../../../../../data_manager/dama_db/postgres/PostgreSQL";

export type NodePgDbConnection = NodePgClient | NodePgPoolClient;

export type ParsedNpmrdsTravelTimesExportTableMetadata = {
  damaViewId: number;

  tableSchema: string;
  tableName: string;

  lastUpdated: string;

  state: string;
  year: number;
  month: number;

  isCompleteWeek: boolean;
  isCompleteMonth: boolean;
  isExpanded: boolean;

  // [ startDate, endDate ]
  data_start_date: string;
  data_end_date: string;
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
