module.exports = {
  "disaster_declarations_summaries_v2": (view_id) => ({
    "name": `disaster_declarations_summaries_v2_${view_id}`,
    "schema": "open_fema_data",
    "columns": [
      {
        "name": "declaration_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "fy_declared",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "fema_declaration_string",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "ih_program_declared",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "hm_program_declared",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "incident_type",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "ia_program_declared",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "declaration_type",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "designated_area",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "id",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "declaration_title",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "incident_begin_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "pa_program_declared",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "last_refresh",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "disaster_number",
        "dataType": "numeric",
        "primaryKey": true
      },
      {
        "name": "place_code",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "disaster_closeout_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "fips_county_code",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "incident_end_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "declaration_request_number",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "fips_state_code",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "state",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "hash",
        "dataType": "text",
        "primaryKey": false
      }
    ]
  }),
  "fema_web_disaster_declarations_v1": (view_id) => ({
    "name": `fema_web_disaster_declarations_v1_${view_id}`,
    "schema": "open_fema_data",
    "columns": [
      {
        "name": "id",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "declaration_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "disaster_name",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "incident_type",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "declaration_type",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "state_code",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "incident_end_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "incident_begin_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "state_name",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "last_refresh",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "disaster_number",
        "dataType": "numeric",
        "primaryKey": true
      },
      {
        "name": "entry_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "hash",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "closeout_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "update_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      }
    ]
  }),
  "fema_web_disaster_summaries_v1": (view_id) => ({
    "name": `fema_web_disaster_summaries_v1_${view_id}`,
    "schema": "open_fema_data",
    "columns": [
      {
        "name": "disaster_number",
        "dataType": "numeric",
        "primaryKey": true
      },
      {
        "name": "total_amount_ihp_approved",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "total_obligated_amount_pa",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "total_amount_ona_approved",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "total_amount_ha_approved",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "pa_load_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "total_obligated_amount_cat_ab", // custom fix
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "id",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "total_number_ia_approved",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "hash",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "total_obligated_amount_cat_c2g",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "last_refresh",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "ia_load_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "total_obligated_amount_hmgp",
        "dataType": "numeric",
        "primaryKey": false
      }
    ]
  }),
  "hazard_mitigation_assistance_mitigated_properties_v2": (view_id) => ({
    "name": `hazard_mitigation_assistance_mitigated_properties_v2_${view_id}`,
    "schema": "open_fema_data",
    "columns": [
      {
        "name": "actual_amount_paid",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "zip",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "city",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "disaster_number",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "county_code",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "region",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "state",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "structure_type",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "county",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "project_identifier",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "type_of_residency",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "state_number_code",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "property_action",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "date_initially_approved",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "number_of_properties",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "subgrantee_tribal_indicator",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "grantee_tribal_indicator",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "number_of_records",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "program_area",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "date_closed",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "title",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "status",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "date_approved",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "program_fy",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "type",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "_i_d",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "damage_category",
        "dataType": "text",
        "primaryKey": false
      }
    ]
  }),
  "hazard_mitigation_assistance_projects_v2": (view_id) => ({
    "name": `hazard_mitigation_assistance_projects_v2_${view_id}`,
    "schema": "open_fema_data",
    "columns": [
      {
        "name": "project_type",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "region",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "state",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "subgrantee",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "project_title",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "grantee_tribal_indicator",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "program_area",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "hash",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "status",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "project_identifier",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "project_counties",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "number_of_properties",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "number_of_records",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "last_refresh",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "number_of_final_properties",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "grantee",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "date_approved",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "benefit_cost_ratio",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "disaster_number",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "county_code",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "state_number_code",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "date_closed",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "county",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "id",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "federal_share_obligated",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "date_initially_approved",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "net_value_benefits",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "cost_share_percentage",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "project_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "subgrantee_tribal_indicator",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "program_fy",
        "dataType": "numeric",
        "primaryKey": false
      }
    ]
  }),
  "housing_assistance_owners_v2": (view_id) => ({
    "name": `housing_assistance_owners_v2_${view_id}`,
    "schema": "open_fema_data",
    "columns": [
      {
        "name": "disaster_number",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "zip_code",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "state",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "average_fema_inspected_damage",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "county",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "city",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "total_damage",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "no_fema_inspected_damage",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "valid_registrations",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "fema_inspected_damage_between1_and10000",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "fema_inspected_damage_between10001_and20000",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "total_inspected",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "fema_inspected_damage_between20001_and30000",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "fema_inspected_damage_greater_than30000",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "approved_for_fema_assistance",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "total_approved_ihp_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "repair_replace_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "rental_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "other_needs_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "approved_between1_and10000",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "approved_between10001_and25000",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "approved_between25001_and_max",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "total_max_grants",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "id",
        "dataType": "text",
        "primaryKey": false
      }
    ]
  }),
  "housing_assistance_renters_v2": (view_id) => ({
    "name": `housing_assistance_renters_v2_${view_id}`,
    "schema": "open_fema_data",
    "columns": [
      {
        "name": "disaster_number",
        "dataType": "numeric",
        "primaryKey": true
      },
      {
        "name": "zip_code",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "total_inspected",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "county",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "city",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "valid_registrations",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "total_inspected_with_no_damage",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "state",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "total_with_moderate_damage",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "total_with_substantial_damage",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "total_with_major_damage",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "approved_for_fema_assistance",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "repair_replace_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "total_approved_ihp_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "rental_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "other_needs_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "approved_between1_and10000",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "approved_between25001_and_max",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "approved_between10001_and25000",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "total_max_grants",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "id",
        "dataType": "text",
        "primaryKey": false
      }
    ]
  }),
  "individual_assistance_housing_registrants_large_disasters_v1": (view_id) => ({
    "name": `individual_assistance_housing_registrants_large_disasters_v1_${view_id}`,
    "schema": "open_fema_data",
    "columns": [
      {
        "name": "disaster_number",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "special_needs",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "damaged_state_abbreviation",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "gross_income",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "damaged_zip_code",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "destroyed",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "roof_damage",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "id",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "foundation_damage_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "tsa_checked_in",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "household_composition",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "own_rent",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "residence_type",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "damaged_city",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "inspected",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "water_level",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "tsa_eligible",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "roof_damage_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "foundation_damage",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "habitability_repairs_required",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "flood_damage",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "rental_assistance_eligible",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "repair_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "sba_eligible",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "replacement_assistance_eligible",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "rental_resource_zip_code",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "rental_resource_city",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "primary_residence",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "personal_property_eligible",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "ppfvl",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "rental_resource_state_abbreviation",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "census_year",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "census_block_id",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "rental_assistance_end_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "replacement_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "flood_insurance",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "repair_assistance_eligible",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "rental_assistance_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "renter_damage_level",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "home_owners_insurance",
        "dataType": "boolean",
        "primaryKey": false
      },
      {
        "name": "rpfvl",
        "dataType": "numeric",
        "primaryKey": false
      }
    ]
  }),
  "individuals_and_households_program_valid_registrations_v1": (view_id) => ({
    "name": `individuals_and_households_program_valid_registrations_v1_${view_id}`,
    "schema": "open_fema_data",
    "columns": [
      {'name':'id', dataType: 'character varying NOT NULL', primaryKey: true},
      {'name':'incident_type', dataType: 'character varying'},
      {'name':'declaration_date', dataType: 'date'},
      {'name':'disaster_number', dataType: 'character varying'},
      {'name':'county', dataType: 'character varying'},
      {'name':'damaged_state_abbreviation', dataType: 'character varying'},
      {'name':'damaged_city', dataType: 'character varying'},
      {'name':'damaged_zip_code', dataType: 'character varying'},
      {'name':'applicant_age', dataType: 'character varying'},
      {'name':'household_composition', dataType: 'character varying'},
      {'name':'occupants_under_two', dataType: 'character varying'},
      {'name':'occupants2to5', dataType: 'character varying'},
      {'name':'occupants6to18', dataType: 'character varying'},
      {'name':'occupants19to64', dataType: 'character varying'},
      {'name':'occupants65and_over', dataType: 'character varying'},
      {'name':'gross_income', dataType: 'character varying'},
      {'name':'own_rent', dataType: 'character varying'},
      {'name':'primary_residence', dataType: 'boolean'},
      {'name':'residence_type', dataType: 'character varying'},
      {'name':'home_owners_insurance', dataType: 'boolean'},
      {'name':'flood_insurance', dataType: 'boolean'},
      {'name':'registration_method', dataType: 'character varying'},
      {'name':'ihp_referral', dataType: 'boolean'},
      {'name':'ihp_eligible', dataType: 'boolean'},
      {'name':'ihp_amount', dataType: 'double precision'},
      {'name':'fip_amount', dataType: 'double precision'},
      {'name':'ha_referral', dataType: 'boolean'},
      {'name':'ha_eligible', dataType: 'boolean'},
      {'name':'ha_amount', dataType: 'double precision'},
      {'name':'ha_status', dataType: 'character varying'},
      {'name':'ona_referral', dataType: 'boolean'},
      {'name':'ona_eligible', dataType: 'boolean'},
      {'name':'ona_amount', dataType: 'double precision'},
      {'name':'utilities_out', dataType: 'boolean'},
      {'name':'home_damage', dataType: 'boolean'},
      {'name':'auto_damage', dataType: 'boolean'},
      {'name':'emergency_needs', dataType: 'boolean'},
      {'name':'food_need', dataType: 'boolean'},
      {'name':'shelter_need', dataType: 'boolean'},
      {'name':'access_functional_needs', dataType: 'boolean'},
      {'name':'sba_eligible', dataType: 'boolean'},
      {'name':'sba_approved', dataType: 'boolean'},
      {'name':'inspn_issued', dataType: 'boolean'},
      {'name':'inspn_returned', dataType: 'boolean'},
      {'name':'habitability_repairs_required', dataType: 'boolean'},
      {'name':'rpfvl', dataType: 'double precision'},
      {'name':'ppfvl', dataType: 'double precision'},
      {'name':'renter_damage_level', dataType: 'character varying'},
      {'name':'destroyed', dataType: 'boolean'},
      {'name':'water_level', dataType: 'double precision'},
      {'name':'high_water_location', dataType: 'character varying'},
      {'name':'flood_damage', dataType: 'boolean'},
      {'name':'flood_damage_amount', dataType: 'double precision'},
      {'name':'foundation_damage', dataType: 'boolean'},
      {'name':'foundation_damage_amount', dataType: 'double precision'},
      {'name':'roof_damage', dataType: 'boolean'},
      {'name':'roof_damage_amount', dataType: 'double precision'},
      {'name':'tsa_eligible', dataType: 'boolean'},
      {'name':'tsa_checked_in', dataType: 'boolean'},
      {'name':'rental_assistance_eligible', dataType: 'boolean'},
      {'name':'rental_assistance_amount', dataType: 'double precision'},
      {'name':'repair_assistance_eligible', dataType: 'boolean'},
      {'name':'repair_amount', dataType: 'double precision'},
      {'name':'replacement_assistance_eligible', dataType: 'boolean'},
      {'name':'replacement_amount', dataType: 'double precision'},
      {'name':'personal_property_eligible', dataType: 'boolean'},
      {'name':'personal_property_amount', dataType: 'double precision'},
      {'name':'ihp_max', dataType: 'boolean'},
      {'name':'ha_max', dataType: 'boolean'},
      {'name':'ona_max', dataType: 'boolean'},
      {'name':'last_refresh', dataType: 'date'}
    ]
  }),
  "public_assistance_applicants_v1": (view_id) => ({
    "name": `public_assistance_applicants_v1_${view_id}`,
    "schema": "open_fema_data",
    "columns": [
      {
        "name": "address_line2",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "city",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "state",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "applicant_name",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "disaster_number",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "address_line1",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "hash",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "last_refresh",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "zip_code",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "applicant_id",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "id",
        "dataType": "text",
        "primaryKey": false
      }
    ]
  }),
  "public_assistance_funded_projects_details_v1": (view_id) => ({
    "name": `public_assistance_funded_projects_details_v1_${view_id}`,
    "schema": "open_fema_data",
    "columns": [
      {
        "name": "disaster_number",
        "dataType": "numeric",
        "primaryKey": true
      },
      {
        "name": "pw_number",
        "dataType": "numeric",
        "primaryKey": true
      },
      {
        "name": "application_title",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "state",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "damage_category",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "federal_share_obligated",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "last_refresh",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "id",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "hash",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "damage_category_code",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "project_size",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "project_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "total_obligated",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "state_code",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "county_code",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "state_number_code",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "dcc",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "applicant_id",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "county",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "declaration_date",
        "dataType": "timestamp with time zone",
        "primaryKey": false
      },
      {
        "name": "incident_type",
        "dataType": "text",
        "primaryKey": false
      },
      {
        "name": "obligated_date",
        "dataType": "timestamp with time zone",
        "primaryKey": true
      }
    ]
  }),
  "registration_intake_individuals_household_programs_v2": (view_id) => ({
    "name": `registration_intake_individuals_household_programs_v2_${view_id}`,
    "schema": "open_fema_data",
    "columns": [
      {
        "name": "disaster_number",
        "dataType": "numeric",
        "primaryKey": true
      },
      {
        "name": "total_valid_registrations",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "state",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "county",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "city",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "valid_call_center_registrations",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "zip_code",
        "dataType": "text",
        "primaryKey": true
      },
      {
        "name": "valid_web_registrations",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "ihp_eligible",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "ihp_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "valid_mobile_registrations",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "ha_referrals",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "ihp_referrals",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "ha_eligible",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "ona_eligible",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "ona_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "ona_referrals",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "ha_amount",
        "dataType": "numeric",
        "primaryKey": false
      },
      {
        "name": "id",
        "dataType": "text",
        "primaryKey": false
      }
    ]
  }),
  "fima_nfip_claims_v1": (view_id) => ({
    "name": `fima_nfip_claims_v1_${view_id}`,
    "schema": "open_fema_data",
    "columns": [
        { "name": "base_flood_elevation",  "dataType": "double precision"},
        { "name": "basement_enclosure_crawlspace_type",  "dataType": "double precision"},
        { "name": "policy_count",  "dataType": "double precision"},
        { "name": "community_rating_system_discount",  "dataType": "double precision"},
        { "name": "elevation_certificate_indicator",  "dataType": "double precision"},
        { "name": "elevation_difference",  "dataType": "double precision"},
        { "name": "location_of_contents",  "dataType": "double precision"},
        { "name": "number_of_floors_in_the_insured_building",  "dataType": "double precision"},
        { "name": "obstruction_type",  "dataType": "double precision"},
        { "name": "occupancy_type",  "dataType": "double precision"},
        { "name": "amount_paid_on_increased_cost_of_compliance_claim",  "dataType": "double precision"},
        { "name": "total_building_insurance_coverage",  "dataType": "double precision"},
        { "name": "total_contents_insurance_coverage",  "dataType": "double precision"},
        { "name": "yearof_loss",  "dataType": "double precision"},
        { "name": "year_of_loss",  "dataType": "double precision"},
        { "name": "latitude",  "dataType": "double precision"},
        { "name": "longitude",  "dataType": "double precision"},
        { "name": "lowest_adjacent_grade",  "dataType": "double precision"},
        { "name": "lowest_floor_elevation",  "dataType": "double precision"},
        { "name": "amount_paid_on_building_claim",  "dataType": "double precision"},
        { "name": "amount_paid_on_contents_claim",  "dataType": "double precision"},
        { "name": "as_of_date",  "dataType": "timestamp with time zone"},
        { "name": "date_of_loss",  "dataType": "timestamp with time zone"},
        { "name": "original_construction_date",  "dataType": "timestamp with time zone"},
        { "name": "original_n_b_date",  "dataType": "timestamp with time zone"},
        { "name": "agriculture_structure_indicator",  "dataType": "boolean"},
        { "name": "elevated_building_indicator",  "dataType": "boolean"},
        { "name": "house_worship",  "dataType": "boolean"},
        { "name": "non_profit_indicator",  "dataType": "boolean"},
        { "name": "post_f_i_r_m_construction_indicator",  "dataType": "boolean"},
        { "name": "small_business_indicator_building",  "dataType": "boolean"},
        { "name": "primary_residence",  "dataType": "boolean"},
        { "name": "reported_city",  "dataType": "character varying"},
        { "name": "condominium_indicator",  "dataType": "character varying"},
        { "name": "county_code",  "dataType": "character varying"},
        { "name": "census_tract",  "dataType": "character varying"},
        { "name": "flood_zone",  "dataType": "character varying"},
        { "name": "rate_method",  "dataType": "character varying"},
        { "name": "state",  "dataType": "character varying"},
        { "name": "reported_zip_code",  "dataType": "character varying"},
        { "name": "id",  "dataType": "character varying", "primaryKey": true}
    ]
  })
}
