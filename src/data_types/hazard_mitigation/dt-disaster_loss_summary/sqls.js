export const ofd = ({
                      table_name, ofd_schema, view_id,
                      pafpd_table, ihp_table, dds_table, sba_table,
                      nfip_table, usda_table,
                    }
) => `
with
   disasters as (
       SELECT disaster_number::text,
              incident_type,
              fips_state_code || fips_county_code          geoid,
      MIN(incident_begin_date)					 incident_begin_date,
      MAX(incident_end_date)						 incident_end_date,
              EXTRACT(YEAR FROM MIN(incident_begin_date))  begin_year,
              EXTRACT(YEAR FROM MAX(incident_end_date))    end_year,
              EXTRACT(MONTH FROM MIN(incident_begin_date)) begin_month,
              EXTRACT(MONTH FROM MAX(incident_end_date))   end_month
       FROM ${ofd_schema}.${dds_table}
       WHERE disaster_number NOT BETWEEN 3000 and  3999
       AND incident_type not in ('Biological', 'Terrorist', 'Other')
       GROUP BY 1, 2, 3
       ORDER BY 1 DESC
     ),
	pa as (
	SELECT disaster_number::text,
           lpad(state_number_code::text, 2, '0') || lpad(county_code::text, 3, '0') geoid,
		   incident_type,
           SUM(COALESCE(project_amount, 0)) project_amount
    FROM ${ofd_schema}.${pafpd_table}
    WHERE dcc NOT IN ('A', 'B', 'Z')
    AND disaster_number NOT BETWEEN 3000 and  3999
    GROUP BY 1, 2, 3
-- 	HAVING SUM(COALESCE(project_amount, 0)) > 0
	  ),
	ihp as (
         SELECT disaster_number,
				SUBSTRING(geoid, 1, 5) geoid,
                incident_type,
                SUM(COALESCE(rpfvl, 0) + COALESCE(ppfvl, 0))                                as ihp_verified_loss
         FROM ${ofd_schema}.${ihp_table}
         WHERE disaster_number::integer NOT BETWEEN 3000 and  3999
         GROUP BY 1, 2, 3
-- 		HAVING SUM(COALESCE(rpfvl, 0) + COALESCE(ppfvl, 0)) > 0
	),
	sba as (SELECT REPLACE(fema_disaster_number, 'DR', '')  disaster_number,
				   SUBSTRING(dd.geoid, 1, 5)                 	geoid,
				   SUM(total_verified_loss)					total_verified_loss
         FROM ${ofd_schema}.${sba_table} sba
         JOIN disasters dd
         ON REPLACE(sba.fema_disaster_number, 'DR', '') = dd.disaster_number
         AND sba.geoid = dd.geoid
		 GROUP BY 1, 2
-- 		 HAVING LENGTH(REPLACE(fema_disaster_number, 'DR', '')) BETWEEN 1 and 4
-- 			AND SUM(total_verified_loss) > 0
		 ORDER BY 1, 2
		   ),
     nfip as (
         SELECT nfip.disaster_number, nfip.geoid, nfip.incident_type, sum(total_amount_paid) total_amount_paid
         FROM ${ofd_schema}.${nfip_table} nfip
         JOIN disasters dd
         ON nfip.disaster_number = dd.disaster_number
         AND nfip.geoid = dd.geoid
         GROUP BY 1, 2, 3
--          HAVING COALESCE(SUM(total_amount_paid), 0) > 0
     ),
    croploss as (
      SELECT usda.disaster_number, usda.geoid, usda.incident_type, sum(indemnity_amount) crop_loss
      FROM ${ofd_schema}.${usda_table} usda
      JOIN disasters d
      ON usda.disaster_number = d.disaster_number
      AND usda.geoid = d.geoid
      GROUP BY 1, 2, 3
--       HAVING sum(indemnity_amount) > 0
    ),
	disaster_declarations_summary as (
		SELECT
			COALESCE(disasters.disaster_number, ihp.disaster_number, pa.disaster_number,
					 sba.disaster_number, nfip.disaster_number, croploss.disaster_number)	    disaster_number,
			COALESCE(disasters.geoid, ihp.geoid, pa.geoid,
					 sba.geoid, nfip.geoid, croploss.geoid) 								                  geoid,
			COALESCE(disasters.incident_type, ihp.incident_type, pa.incident_type,
					 nfip.incident_type, croploss.incident_type)			                        incident_type,
			MIN(incident_begin_date)                                                   		fema_incident_begin_date,
			MAX(incident_end_date)                                                     		fema_incident_end_date,
			SUM(COALESCE(ihp_verified_loss, 0)) 											                    ihp_loss,
			SUM(COALESCE(project_amount, 0)) 												                      pa_loss,
			SUM(COALESCE(total_verified_loss, 0)) 											                  sba_loss,
			SUM(COALESCE(total_amount_paid, 0)) 											                    nfip_loss,
			SUM(COALESCE(ihp_verified_loss, 0) + COALESCE(project_amount, 0) +
				COALESCE(total_verified_loss, 0) + COALESCE(total_amount_paid, 0))          fema_property_damage,
			SUM(COALESCE(crop_loss, 0))                                                   fema_crop_damage
		FROM ihp
		FULL OUTER JOIN pa
		USING (disaster_number, geoid)
		FULL OUTER JOIN sba
		USING (disaster_number, geoid)
		FULL OUTER JOIN nfip
		USING (disaster_number, geoid)
		FULL OUTER JOIN croploss
		USING (disaster_number, geoid)
		FULL OUTER JOIN disasters
		USING (disaster_number, geoid)
		GROUP BY 1, 2, 3
	)

SELECT disaster_number, geoid,
       CASE
         WHEN lower(incident_type) = 'coastal storm'                                    THEN 'coastal'
         WHEN lower(incident_type) IN ('dam/levee break', 'flood', 'heavy rain')        THEN 'riverine'
         WHEN lower(incident_type) = 'drought'                                          THEN 'drought'
         WHEN lower(incident_type) = 'fire'                                             THEN 'wildfire'
         WHEN lower(incident_type) = 'freezing'                                         THEN 'coldwave'
         WHEN lower(incident_type) IN ('hurricane', 'typhoon', 'severe storm',
                                       'severe storm(s)')                               THEN 'hurricane'
         WHEN lower(incident_type) = 'mud/landslide'                                    THEN 'landslide'
         WHEN lower(incident_type) = 'severe ice storm'                                 THEN 'icestorm'
         WHEN lower(incident_type) IN ('snow', 'freezing fog', 'snowstorm')             THEN 'winterweat'
         WHEN lower(incident_type) = 'earthquake'                                       THEN 'earthquake'
         WHEN lower(incident_type) = 'tornado'                                          THEN 'tornado'
         WHEN lower(incident_type) = 'tsunami'                                          THEN 'tsunami'
         WHEN lower(incident_type) = 'volcanic eruption'                                THEN 'volcano'
       END incident_type,
       fema_incident_begin_date, fema_incident_end_date,
       ihp_loss, pa_loss, sba_loss, nfip_loss,
       fema_property_damage, fema_crop_damage
INTO  ${ofd_schema}.${table_name}_${view_id}
FROM disaster_declarations_summary;
`;
