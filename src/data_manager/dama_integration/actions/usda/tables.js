const floatColumns = [
  'policies_earning_premium', 'net_planted_acres', 'net_endorsed_acres', 'liability', 'total_premium', 'producer_paid_premium',
  'subsidy', 'state_or_private_subsidy', 'additional_subsidy', 'efa_premium_discount', 'net_determined_acres', 'indemnity_amount',
  'loss_ratio'
]
const tables = {
    usda_crop_insurance_cause_of_loss: (view_id) => ({
        name: `usda_crop_insurance_cause_of_loss_${view_id}`,
        schema: 'open_fema_data',
        columns: [
            'commodity_year_identifier', 'state_fips', 'state_abbr', 'county_fips', 'county_name', 'commodity_code', 'commodity_name',
            'insurance_plan_code', 'insurance_plan_name_abbr', 'coverage_category', 'stage_code', 'cause_of_loss_code', 'cause_of_loss_desc',
            'month_of_loss', 'month_of_loss_name', 'year_of_loss',

            'policies_earning_premium', 'policies_indemnified', 'net_planted_acres', 'net_endorsed_acres', 'liability', 'total_premium', 'producer_paid_premium',
            'subsidy', 'state_or_private_subsidy', 'additional_subsidy', 'efa_premium_discount', 'net_determined_acres', 'indemnity_amount',
            'loss_ratio'
        ].map(col => ({
          name: col.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`),
          dataType:
            // tables.usda_crop_insurance_cause_of_loss.numericColumns.includes(col) ? 'double precision' :
            floatColumns.includes(col) ? 'double precision'
              // tables.usda_crop_insurance_cause_of_loss.dateColumns.includes(col) ? 'timestamp with time zone' :
              //   tables.usda_crop_insurance_cause_of_loss.booleanColumns.includes(col) ? 'boolean'
                  : 'character varying'
        })),
        numericColumns: [

        ],
        floatColumns: [
            'policies_earning_premium', 'net_planted_acres', 'net_endorsed_acres', 'liability', 'total_premium', 'producer_paid_premium',
            'subsidy', 'state_or_private_subsidy', 'additional_subsidy', 'efa_premium_discount', 'net_determined_acres', 'indemnity_amount',
            'loss_ratio'
        ],
        dateColumns: [

        ],
        booleanColumns: [

        ]
    })
}


module.exports = {
    tables
}
