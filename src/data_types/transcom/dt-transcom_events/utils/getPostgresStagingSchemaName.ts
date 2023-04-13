export default function getPostgresStagingSchemaName(etl_context_id: number) {
  return `staging_transcom_eci_${etl_context_id}`;
}
