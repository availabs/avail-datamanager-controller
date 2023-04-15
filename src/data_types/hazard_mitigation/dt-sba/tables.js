const numericColumns = ["year", "entry_id"],
  floatColumns = [
    "total_verified_loss",
    "verified_loss_real_estate",
    "verified_loss_content",
    "total_approved_loan_amount",
    "approved_amount_real_estate",
    "approved_amount_content",
    "approved_amount_eidl",
  ],
  dateColumns = ["fema_date"];

const tables = {
    sba_disaster_loan_data_new: (view_id) => ({
        name: `sba_disaster_loan_data_new_${view_id}`,
        schema: "open_fema_data",
        columns: [
            "year",
            "sba_physical_declaration_number",
            "sba_eidl_declaration_number",
            "fema_disaster_number",
            "sba_disaster_number",
            "damaged_property_city_name",
            "damaged_property_zip_code",
            "damaged_property_county_or_parish_name",
            "damaged_property_state_code",

            "total_verified_loss",
            "verified_loss_real_estate",
            "verified_loss_content",
            "total_approved_loan_amount",
            "approved_amount_real_estate",
            "approved_amount_content",
            "approved_amount_eidl",
            "loan_type",
            "incidenttype",
            "geoid",
            "fema_date",
            "entry_id",
        ].map(col => ({
          name: col,
          dataType:
          numericColumns.includes(col) ? "double precision" :
            floatColumns.includes(col) ? "double precision" :
              dateColumns.includes(col) ? "timestamp with time zone" : "character varying",
        })),
        numericColumns,
        floatColumns,
        dateColumns,
    }),
};


module.exports = {
    tables
}
