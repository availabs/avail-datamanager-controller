//  If we make this uniform across all EtlContexts, we can have a cleanup task
//    that DROPs staging schemas for DONE tasks.

export default function getPostgresStagingSchemaName(etl_context_id: number) {
  return `staging_eci_${etl_context_id}`;
}
