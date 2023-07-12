const grid_fn = () => `
DROP FUNCTION IF EXISTS makegrid_2d(geometry,integer,integer);
CREATE OR REPLACE FUNCTION public.makegrid_2d (
    bound_polygon public.geometry,
    width_step integer,
    height_step integer
)
    RETURNS public.geometry AS
$body$
DECLARE
    Xmin DOUBLE PRECISION;
    Xmax DOUBLE PRECISION;
    Ymax DOUBLE PRECISION;
    X DOUBLE PRECISION;
    Y DOUBLE PRECISION;
    NextX DOUBLE PRECISION;
    NextY DOUBLE PRECISION;
    CPoint public.geometry;
    sectors public.geometry[];
    i INTEGER;
    SRID INTEGER;
BEGIN
    Xmin := ST_XMin(bound_polygon);
    Xmax := ST_XMax(bound_polygon);
    Ymax := ST_YMax(bound_polygon);
    SRID := ST_SRID(bound_polygon);

    Y := ST_YMin(bound_polygon); --current sector's corner coordinate
    i := -1;
    <<yloop>>
    LOOP
        IF (Y > Ymax) THEN
            EXIT;
        END IF;

        X := Xmin;
        <<xloop>>
        LOOP
            IF (X > Xmax) THEN
                EXIT;
            END IF;

            CPoint := ST_SetSRID(ST_MakePoint(X, Y), SRID);
            NextX := ST_X(ST_Project(CPoint, $2, radians(90))::geometry);
            NextY := ST_Y(ST_Project(CPoint, $3, radians(0))::geometry);

            i := i + 1;
            sectors[i] := ST_MakeEnvelope(X, Y, NextX, NextY, SRID);

            X := NextX;
        END LOOP xloop;
        CPoint := ST_SetSRID(ST_MakePoint(X, Y), SRID);
        NextY := ST_Y(ST_Project(CPoint, $3, radians(0))::geometry);
        Y := NextY;
    END LOOP yloop;

    RETURN ST_Collect(sectors);
END;
$body$
    LANGUAGE 'plpgsql';
`;

const grid = (state_schema, state_table) => `
    SELECT cell
    FROM (
             SELECT (
                        ST_Dump(
                                makegrid_2d(
                                        (
                                            SELECT st_buffer(ST_Collect(ST_Simplify(st_setsrid(geom, 4326), 0.2)), 2, 'side=left') FROM ${state_schema}.${state_table}
                                            where geoid not in ('11', '99', '72', '69', '60', '66', '78')
                                              and geoid not in ('02', '15')
                                        ),
                                        196000, -- width step in meters
                                        196000  -- height step in meters
                                    )
                            )
                        ) .geom AS cell
         ) grid
             JOIN (
        SELECT st_buffer(ST_Collect(ST_Simplify(st_setsrid(geom, 4326), 0.2)), 2, 'side=left') geom FROM ${state_schema}.${state_table}
        where geoid not in ('11', '99', '72', '69', '60', '66', '78')
          and geoid not in ('02', '15')
    ) area
                  ON st_intersects(grid.cell, area.geom)

    union

    SELECT cell
    FROM (
             SELECT (
                        ST_Dump(
                                makegrid_2d(
                                        (
                                            SELECT st_setsrid(geom, 4326) FROM ${state_schema}.${state_table}
                                            WHERE geoid in ('02')
                                        ),
                                        196000, -- width step in meters
                                        196000  -- height step in meters
                                    )
                            )
                        ) .geom AS cell
         ) grid
             JOIN (
        SELECT st_setsrid(geom, 4326) geom FROM ${state_schema}.${state_table}
        where geoid in ('02')
    ) area
                  ON st_intersects(grid.cell, area.geom)



    union

    SELECT cell
    FROM (
             SELECT (
                        ST_Dump(
                                makegrid_2d(
                                        (
                                            SELECT ST_Collect(ST_Simplify(st_setsrid(geom, 4326), 0.1)) FROM ${state_schema}.${state_table}
                                            WHERE geoid in ('15')
                                        ),
                                        196000, -- width step in meters
                                        196000  -- height step in meters
                                    )
                            )
                        ) .geom AS cell
         ) grid
             JOIN (
        SELECT ST_Collect(ST_Simplify(st_setsrid(geom, 4326), 0.1)) geom FROM ${state_schema}.${state_table}
        where geoid in ('15')
    ) area
                  ON st_intersects(grid.cell, area.geom)
`;

const fips_to_regions_and_surrounding_counties_hurricane = (county_schema, county_table) => `
    SELECT distinct substring(geoid, 1, 5) fips,
                    CASE

                        WHEN substring(geoid, 1, 5) IN
                             ('48215', '48061', '48489', '48261', '48273', '48355', '48409', '48007', '48057', '48321',
                              '48039', '48071', '48245', '48361', '22023', '22019', '22053', '22113', '22045', '22101',
                              '22109', '22057', '22051', '22075', '22087', '22071', '22103', '28045', '28047', '28059',
                              '48167', '48047', '48249', '48175', '48025', '48469', '48239', '48481', '48157', '48201',
                              '48291', '48391', '22007', '22005', '22093', '22095', '22089', '22063', '22105', '22117',
                              '28109', '28131', '28039', '01097', '22099', '22001', '22055', '22047', '01003', '01053',
                              '01039', '12033', '12113', '12091', '12131', '12005', '12133', '12059', '12045', '12017',
                              '12053', '12101', '12057', '12081', '12115', '12049', '12027', '12015', '12071', '12021',
                              '12087', '12086', '12011', '12099', '12085', '12111', '12009', '12061', '12103', '37055',
                              '37177', '37187', '37095', '37137', '37049', '37031', '37133', '37141', '37129', '37019',
                              '45051', '45043', '45013', '45019', '45035', '45015', '37103', '37013', '45053', '13051',
                              '13029', '13179', '13191')
                            THEN ('x')
                        WHEN substring(geoid, 1, 5) IN
                             ('48297', '48255', '48123', '48285', '48089', '48015', '48477', '48313', '48185', '48473',
                              '48471', '48339', '48407', '48455', '48373', '48457', '48199', '48241', '48351', '22115',
                              '22011', '22079', '22039', '22003', '22097', '22009', '22077', '22121', '22033', '22037',
                              '22125', '28157', '28005', '28113', '28147', '28091', '28073', '28035', '28111', '28041',
                              '28031', '28067', '28153', '28023', '01023', '01129', '01025', '01099', '01035', '01013',
                              '01041', '01109', '01031', '01045', '01061', '01069', '13201', '13099', '01067', '12063',
                              '13253', '13087', '12039', '12073', '12129', '12037', '12077', '12013', '13131', '13275',
                              '12065', '12079', '12123', '12067', '12121', '12003', '12125', '12007', '12001', '12075',
                              '13127', '13039', '13049', '12089', '12031', '12107', '12035', '12083', '12127', '12047',
                              '12023', '12019', '12109', '12119', '12069', '12117', '12095', '12051', '12043', '12055',
                              '12097', '12093', '12105', '12049', '13229', '13025', '13305', '13183', '13267', '13109',
                              '13001', '13103', '13251', '13031', '45005', '45049', '45029', '45009', '45075', '45027',
                              '45089', '45041', '45067', '37155', '45033', '37047', '37017', '37061', '37107', '37163',
                              '37191', '37079', '37147', '37117', '37015', '37091', '37073', '51800', '51093', '51550',
                              '51810', '37053', '37029', '37139', '37143', '37041', '12029', '12041'
                                 )
                            THEN ('y')
                        WHEN substring(geoid, 1, 2) IN
                             ('23', '33', '50', '36', '25', '09', '44', '26', '39', '42',
                              '34', '54', '21', '10', '24', '17', '18', '47', '05', '40')
                            THEN ('z')
                        WHEN substring(geoid, 1, 5) IN
                             (
                              '20107', '20011', '20037', '20021', '20001', '20133', '20099', '20125', '20019', '20049',
                              '20015', '20035', '20173', '20191', '20095', '20077', '20007', '20033', '20025', '48357',
                              '48295', '48393', '48211', '48179', '48483', '48129', '48087', '48075', '48369', '48437',
                              '48191', '48045', '35041', '35025', '35005', '35015', '35035', '35051', '35013', '35029',
                              '35023', '35017', '04001', '04011', '04009', '04003', '04007', '04021', '04013', '04019',
                              '04012', '04027', '06065', '06025', '06073', '04023', '48311', '48013', '48493', '48177',
                              '48149', '48051', '48041', '48289', '48225', '48005', '48403', '48131', '48247', '48427',
                              '48405', '48347', '48479', '48505', '48283', '48127', '48017', '48279', '48189', '48153',
                              '48345', '48101', '48197', '48487', '48485', '48077', '48337', '48097', '48181', '48147',
                              '48277', '48387', '48037', '48067', '48343', '48449', '48159', '48223', '48231', '48085',
                              '48121', '48497', '48237', '48503', '48447', '48207', '48433', '48263', '48169', '48305',
                              '48445', '48501', '48079', '48219', '48303', '48107', '48125', '48269', '48275', '48155',
                              '48023', '48009', '48119', '48063', '48315', '48203', '48183', '48459', '48499', '48379',
                              '48365', '48419', '48401', '48073', '48423', '48213', '48001', '48161', '48293', '48349',
                              '48217', '48139', '48251', '48221', '48425', '48143', '48133', '48093', '48059', '48441',
                              '48353', '48335', '48227', '48317', '48003', '48165', '48115', '48033', '48415', '48151',
                              '48253', '48417', '48429', '48363', '48367', '48439', '48113', '48397', '48257', '48467',
                              '48109', '48389', '48301', '48495', '48135', '48329', '48173', '48431', '48081', '48399',
                              '48083', '48049', '48475', '48103', '48461', '48383', '48235', '48451', '48095', '48307',
                              '48411', '48299', '48319', '48327', '48267', '48413', '48435', '48105', '48465', '48265',
                              '48019', '48463', '48323', '48507', '48163', '48325', '48029', '48187', '48091', '48209',
                              '48055', '48453', '48021', '48491', '48027', '48331', '48287', '48395', '48145', '48309',
                              '48035', '48333', '48193', '48099', '48281', '48053', '48171', '48259', '48137', '48385',
                              '48271', '48371', '48443', '48043', '48377', '48243', '48141', '48229', '48031', '22029',
                              '22025', '22059', '22043', '22069', '22085', '28001', '28037', '28085', '28077', '28065',
                              '28127', '28129', '28061', '28101', '28123', '28075', '01119', '01091', '01131', '01085',
                              '01101', '01011', '01005', '13239', '13061', '13243', '13037', '13007', '13205', '13071',
                              '13027', '13185', '13101', '13065', '13005', '13069', '13161', '13279', '13209', '13107',
                              '13043', '13033', '13245', '13003', '13299', '45003', '45011', '45063', '45079', '45017',
                              '45085', '45055', '45061', '45031', '45025', '45069', '37165', '37093', '37085', '37051',
                              '37101', '37183', '37069', '37127', '37195', '37065', '37083', '37131', '51175', '51183',
                              '51181', '51740', '51710', '51131', '51001', '51650', '51700', '51095', '51073', '51115',
                              '51119', '51103', '51133', '51193', '51159', '51099', '22017', '22015', '22031', '22081',
                              '22119', '22027', '22111', '22067', '22123', '22035', '22065', '22107', '22041', '22083',
                              '22073', '22021', '22049', '22127', '22061', '22013', '28141', '28003', '28139', '28009',
                              '28093', '28033', '28137', '28143', '28027', '28119', '28107', '28071', '28145', '28115',
                              '28081', '28057', '28087', '28103', '28069', '28099', '28079', '28121', '28089', '28049',
                              '28029', '28021', '28149', '28163', '28051', '28015', '28097', '28155', '28013', '28025',
                              '28117', '28017', '28095', '28019', '28105', '28159', '28007', '28011', '28133', '28151',
                              '28053', '28055', '28135', '28161', '28043', '28083', '28125', '28063', '01107', '01075',
                              '01057', '01125', '01063', '01065', '01007', '01105', '01047', '01001', '01021', '01037',
                              '01051', '01123', '01017', '01081', '01113', '01087', '01111', '01027', '01121', '01117',
                              '01073', '01009', '01055', '01015', '01029', '01019', '01049', '01071', '01089', '01095',
                              '01083', '01103', '01077', '01079', '01033', '01059', '01133', '01093', '01127', '01043',
                              '01115', '13083', '13295', '13047', '13313', '13213', '13111', '13123', '13291', '13241',
                              '13137', '13311', '13187', '45073', '13119', '13011', '13139', '13157', '13059', '13195',
                              '13221', '13317', '13181', '13073', '13189', '13301', '13265', '13141', '13167', '13283',
                              '13175', '13091', '13289', '13149', '13285', '13145', '13215', '13053', '13259', '13307',
                              '13273', '13261', '13177', '13095', '13321', '13081', '13315', '13017', '13155', '13277',
                              '13019', '13173', '13075', '13271', '13309', '13287', '13319', '13303', '13125', '13163',
                              '13153', '13235', '13093', '13193', '13269', '13263', '13293', '13079', '13207', '13171',
                              '13231', '13199', '13077', '13113', '13063', '13151', '13247', '13217', '13255', '13237',
                              '13133', '13219', '13013', '13135', '13121', '13117', '13057', '13015', '13115', '13055',
                              '13129', '13227', '13085', '13233', '13143', '13045', '13097', '13223', '13067', '13089',
                              '13297', '13211', '13159', '13035', '13169', '13009', '13021', '13225', '13023', '13197',
                              '13249', '13281', '13257', '13147', '13105', '45077', '45007', '45045', '45001', '45059',
                              '45047', '45065', '45037', '45081', '45071', '45039', '45023', '45091', '45021', '45087',
                              '45083', '45057', '37039', '37075', '37113', '37043', '37173', '37087', '37099', '37175',
                              '37089', '37021', '37115', '37199', '37111', '37161', '37149', '37045', '37023', '37011',
                              '37189', '37027', '37193', '37009', '37005', '37171', '37197', '37169', '37067', '37057',
                              '37159', '37059', '37097', '37119', '37025', '37167', '37179', '37007', '37153', '37123',
                              '37125', '37105', '37037', '37001', '37135', '37063', '37033', '37145', '37077', '37181',
                              '37185', '37121', '37003', '37035', '37109', '37071', '37157', '37081', '37151', '51169',
                              '51195', '51051', '51027', '51167', '51185', '51191', '51173', '51021', '51197', '51077',
                              '51035', '51155', '51071', '51121', '51063', '51067', '51161', '51045', '51005', '51023',
                              '51019', '51143', '51083', '51031', '51009', '51163', '51015', '51125', '51029', '51011',
                              '51147', '51037', '51111', '51117', '51105', '51141', '51089', '51680', '51770', '51775',
                              '51580', '51678', '51530', '51690', '51590', '51720', '51520', '51640', '51750', '51790',
                              '51820', '51017', '51091', '51165', '51660', '51003', '51540', '51171', '51187', '51139',
                              '51157', '51047', '51113', '51061', '51683', '51179', '51177', '51109', '51079', '51137',
                              '51069', '51840', '51043', '51107', '51153', '51600', '51059', '51510', '51013', '11001',
                              '51685', '51610', '51630', '51065', '51075', '51049', '51145', '51036', '51127', '51101',
                              '51097', '51057', '51033', '51025', '51081', '51053', '51149', '51730', '51135', '51007',
                              '51041', '51760', '51087', '51085', '51570', '51670', '51595', '35027', '55123', '55081',
                              '55057', '55001', '55137', '55077', '55139', '55039', '55015', '55071', '55117', '55009',
                              '55061', '55049', '55087', '55047', '55023', '55103', '55043', '55111', '55065', '55021',
                              '55025', '55027', '55131', '55089', '55079', '55133', '55055', '55101', '55059', '55127',
                              '55105', '55045', '27055', '19005', '19191', '19089', '19037', '19067', '19033', '19197',
                              '19091', '19025', '19187', '19079', '19083', '19075', '19013', '19019', '19055', '19061',
                              '19069', '19023', '19017', '19065', '19043', '19027', '19073', '19015', '19169', '19127',
                              '19171', '19011', '19113', '19105', '19097', '19045', '19163', '19031', '19139', '19103',
                              '19095', '19157', '19099', '19153', '19049', '19077', '19009', '19029', '19001', '19121',
                              '19181', '19125', '19123', '19107', '19183', '19115', '19057', '19111', '19087', '19101',
                              '19177', '19179', '19051', '19135', '19117', '19039', '19053', '19175', '19159', '19003',
                              '19173', '19185', '19007', '29227', '29081', '29129', '29171', '29211', '29001', '29197',
                              '29199', '29103', '29045', '29111', '29127', '29173', '29163', '29113', '29183', '29189',
                              '29099', '29071', '29073', '29151', '29027', '29019', '29089', '29079', '29115', '29041',
                              '29121', '29175', '29205', '29137', '29007', '29139', '29219', '29510', '29195', '29107',
                              '29037', '29013', '29217', '29011', '29097', '29145', '29119', '29009', '29109', '29057',
                              '29039', '29185', '29083', '29101', '29159', '29141', '29015', '29085', '29167', '29077',
                              '29043', '29213', '29209', '29153', '29067', '29225', '29229', '29215', '29091', '29149',
                              '29181', '29035', '29203', '29179', '29065', '29093', '29221', '29055', '29161', '29125',
                              '29169', '29131', '29051', '29069', '29155', '29143', '29223', '29133', '29201', '29031',
                              '29017', '29059', '29105', '29029', '29053', '29135', '29187', '29186', '29157', '29207',
                              '29023', '29123'
                                 )
                            THEN ('z')
                        ELSE 'UNCLASSIFIED'
                        END                region
    FROM (select distinct substring(geoid, 1, 5) geoid from ${county_schema}.${county_table} where geoid is not null) s
    where geoid is not null
`;

const fips_to_regions_and_surrounding_counties = (county_schema, county_table) => `
SELECT
               distinct substring(geoid, 1, 5) fips,
                        CASE
                            WHEN substring(geoid, 1, 2) IN ('11', '23', '33', '25', '50', '44', '09', '34', '10', '24', '42', '36', '51', '54', '10')
                                THEN 'A'

                            WHEN substring(geoid, 1, 2) IN ('37', '45', '13', '12', '01', '28', '47', '21')
                                THEN 'B'

                            WHEN substring(geoid, 1, 2) IN ('27', '55', '26', '17', '18', '39')
                                THEN 'C'

                            WHEN substring(geoid, 1, 2) IN ('30', '38', '56', '46', '49', '08')
                                THEN 'D'

                            WHEN substring(geoid, 1, 2) IN ('19', '29', '20', '31')
                                THEN 'E'

                            WHEN substring(geoid, 1, 2) IN ('48', '22', '35', '40', '05')
                                THEN 'F'

                            WHEN substring(geoid, 1, 2) IN ('06', '04', '32', '15')
                                THEN 'G'

                            WHEN substring(geoid, 1, 2) IN ('53', '16', '41', '02')
                                THEN 'H'

                            ELSE 'UNCLASSIFIED'
                            END region
           FROM (select distinct substring(geoid, 1, 5) geoid from ${county_schema}.${county_table} where geoid is not null) s
           where geoid is not null
`
export const hlr = ({
                      table_name, view_id,
                      state_schema, state_table,
                      county_schema, county_table,
                      ncei_schema, ncei_table,
                      pb_schema, pb_table,
                      nri_schema, nri_table,
                      startYear, endYear
                    }
) => `
${grid_fn()}

with grid as (${grid(state_schema, state_table)}),
    tmp_grid_new as (
      select row_number() OVER() id, cell
      from grid
    ),
    fips_to_regions_and_surrounding_counties as (${fips_to_regions_and_surrounding_counties(county_schema, county_table)}),
    fips_to_regions_and_surrounding_counties_hurricane as (${fips_to_regions_and_surrounding_counties_hurricane(county_schema, county_table)}),
    county_geo as (
      select st_makevalid(geom) geom, geoid
      from ${county_schema}.${county_table}
    ),
    tmp_fips_to_grid_mapping_196_newer as (
      SELECT fips, grid.id, ST_Area(ST_INTERSECTION(grid.cell, county_geo.geom)) covered_area,
      rank() over (partition by fips order by ST_Area(ST_INTERSECTION(grid.cell, county_geo.geom)) desc)
      FROM fips_to_regions_and_surrounding_counties  county
      JOIN county_geo
      ON fips = county_geo.geoid
      JOIN tmp_grid_new grid
      ON NOT st_disjoint(county_geo.geom, grid.cell)
      order by 1, 3 desc
    ),
    lrpbs as (
    SELECT pb.*,
           CASE
               WHEN ctype = 'buildings'
                   THEN
                   coalesce(CASE
                                WHEN nri_category IN ('wind') -- NRI uses old data for exposure, so EXPB should only be used until the year NRI pulls data for. After that year, coalesce(damage_adjusted, 0) should be used if it's > expb
                                    THEN LEAST(coalesce(damage_adjusted, 0), SWND_EXPB):: double precision / NULLIF (SWND_EXPB, 0)
                                WHEN nri_category IN ('wildfire')
                                    THEN LEAST(coalesce(damage_adjusted, 0), WFIR_EXPB):: double precision / NULLIF (WFIR_EXPB, 0)
                                WHEN nri_category IN ('tsunami')
                                    THEN LEAST(coalesce(damage_adjusted, 0), TSUN_EXPB):: double precision / NULLIF (TSUN_EXPB, 0)
                                WHEN nri_category IN ('tornado')
                                    THEN LEAST(coalesce(damage_adjusted, 0), TRND_EXPB):: double precision / NULLIF (TRND_EXPB, 0)
                                WHEN nri_category IN ('riverine')
                                    THEN LEAST(coalesce(damage_adjusted, 0), RFLD_EXPB):: double precision / NULLIF (RFLD_EXPB, 0)
                                WHEN nri_category IN ('lightning')
                                    THEN LEAST(coalesce(damage_adjusted, 0), LTNG_EXPB):: double precision / NULLIF (LTNG_EXPB, 0)
                                WHEN nri_category IN ('landslide')
                                    THEN LEAST(coalesce(damage_adjusted, 0), LNDS_EXPB):: double precision / NULLIF (LNDS_EXPB, 0)
                                WHEN nri_category IN ('icestorm')
                                    THEN LEAST(coalesce(damage_adjusted, 0), ISTM_EXPB):: double precision / NULLIF (ISTM_EXPB, 0)
                                WHEN nri_category IN ('hurricane')
                                    THEN LEAST(coalesce(damage_adjusted, 0), HRCN_EXPB):: double precision / NULLIF (HRCN_EXPB, 0)
                                WHEN nri_category IN ('heatwave')
                                    THEN LEAST(coalesce(damage_adjusted, 0), HWAV_EXPB):: double precision / NULLIF (HWAV_EXPB, 0)
                                WHEN nri_category IN ('hail')
                                    THEN LEAST(coalesce(damage_adjusted, 0), HAIL_EXPB):: double precision / NULLIF (HAIL_EXPB, 0)
                                WHEN nri_category IN ('avalanche')
                                    THEN LEAST(coalesce(damage_adjusted, 0), AVLN_EXPB):: double precision / NULLIF (AVLN_EXPB, 0)
                                WHEN nri_category IN ('coldwave')
                                    THEN LEAST(coalesce(damage_adjusted, 0), CWAV_EXPB):: double precision / NULLIF (CWAV_EXPB, 0)
                                WHEN nri_category IN ('winterweat')
                                    THEN LEAST(coalesce(damage_adjusted, 0), WNTW_EXPB):: double precision / NULLIF (WNTW_EXPB, 0)
                                WHEN nri_category IN ('volcano')
                                    THEN LEAST(coalesce(damage_adjusted, 0), VLCN_EXPB):: double precision / NULLIF (VLCN_EXPB, 0)
                                WHEN nri_category IN ('coastal')
                                    THEN LEAST(coalesce(damage_adjusted, 0), CFLD_EXPB):: double precision / NULLIF (CFLD_EXPB, 0)
                                END, 0)
               WHEN ctype = 'crop'
                   THEN
                   coalesce(CASE
                                WHEN nri_category IN ('wind')
                                    THEN LEAST(coalesce(damage_adjusted, 0), SWND_EXPA):: double precision / NULLIF (SWND_EXPA, 0)
                                WHEN nri_category IN ('wildfire')
                                    THEN LEAST(coalesce(damage_adjusted, 0), WFIR_EXPA):: double precision / NULLIF (WFIR_EXPA, 0)
                                WHEN nri_category IN ('tornado')
                                    THEN LEAST(coalesce(damage_adjusted, 0), TRND_EXPA):: double precision / NULLIF (TRND_EXPA, 0)
                                WHEN nri_category IN ('riverine')
                                    THEN LEAST(coalesce(damage_adjusted, 0), RFLD_EXPA):: double precision / NULLIF (RFLD_EXPA, 0)
                                WHEN nri_category IN ('hurricane')
                                    THEN LEAST(coalesce(damage_adjusted, 0), HRCN_EXPA):: double precision / NULLIF (HRCN_EXPA, 0)
                                WHEN nri_category IN ('heatwave')
                                    THEN LEAST(coalesce(damage_adjusted, 0), HWAV_EXPA):: double precision / NULLIF (HWAV_EXPA, 0)
                                WHEN nri_category IN ('hail')
                                    THEN LEAST(coalesce(damage_adjusted, 0), HAIL_EXPA):: double precision / NULLIF (HAIL_EXPA, 0)
                                WHEN nri_category IN ('drought')
                                    THEN LEAST(coalesce(damage_adjusted, 0), DRGT_EXPA):: double precision / NULLIF (DRGT_EXPA, 0)
                                WHEN nri_category IN ('coldwave')
                                    THEN LEAST(coalesce(damage_adjusted, 0), CWAV_EXPA):: double precision / NULLIF (CWAV_EXPA, 0)
                                WHEN nri_category IN ('winterweat')
                                    THEN LEAST(coalesce(damage_adjusted, 0), WNTW_EXPA):: double precision / NULLIF (WNTW_EXPA, 0)
                                END, 0)
               WHEN ctype = 'population'
                   THEN
                   coalesce(CASE
                                WHEN nri_category IN ('wind')
                                    THEN LEAST(coalesce(damage_adjusted, 0), SWND_EXPPE):: double precision / NULLIF (SWND_EXPPE, 0)
                                WHEN nri_category IN ('wildfire')
                                    THEN LEAST(coalesce(damage_adjusted, 0), WFIR_EXPPE):: double precision / NULLIF (WFIR_EXPPE, 0)
                                WHEN nri_category IN ('tsunami')
                                    THEN LEAST(coalesce(damage_adjusted, 0), TSUN_EXPPE):: double precision / NULLIF (TSUN_EXPPE, 0)
                                WHEN nri_category IN ('tornado')
                                    THEN LEAST(coalesce(damage_adjusted, 0), TRND_EXPPE):: double precision / NULLIF (TRND_EXPPE, 0)
                                WHEN nri_category IN ('riverine')
                                    THEN LEAST(coalesce(damage_adjusted, 0), RFLD_EXPPE):: double precision / NULLIF (RFLD_EXPPE, 0)
                                WHEN nri_category IN ('lightning')
                                    THEN LEAST(coalesce(damage_adjusted, 0), LTNG_EXPPE):: double precision / NULLIF (LTNG_EXPPE, 0)
                                WHEN nri_category IN ('landslide')
                                    THEN LEAST(coalesce(damage_adjusted, 0), LNDS_EXPPE):: double precision / NULLIF (LNDS_EXPPE, 0)
                                WHEN nri_category IN ('icestorm')
                                    THEN LEAST(coalesce(damage_adjusted, 0), ISTM_EXPPE):: double precision / NULLIF (ISTM_EXPPE, 0)
                                WHEN nri_category IN ('hurricane')
                                    THEN LEAST(coalesce(damage_adjusted, 0), HRCN_EXPPE):: double precision / NULLIF (HRCN_EXPPE, 0)
                                WHEN nri_category IN ('heatwave')
                                    THEN LEAST(coalesce(damage_adjusted, 0), HWAV_EXPPE):: double precision / NULLIF (HWAV_EXPPE, 0)
                                WHEN nri_category IN ('hail')
                                    THEN LEAST(coalesce(damage_adjusted, 0), HAIL_EXPPE):: double precision / NULLIF (HAIL_EXPPE, 0)
                                WHEN nri_category IN ('avalanche')
                                    THEN LEAST(coalesce(damage_adjusted, 0), AVLN_EXPPE):: double precision / NULLIF (AVLN_EXPPE, 0)
                                WHEN nri_category IN ('coldwave')
                                    THEN LEAST(coalesce(damage_adjusted, 0), CWAV_EXPPE):: double precision / NULLIF (CWAV_EXPPE, 0)
                                WHEN nri_category IN ('winterweat')
                                    THEN LEAST(coalesce(damage_adjusted, 0), WNTW_EXPPE):: double precision / NULLIF (WNTW_EXPPE, 0)
                                WHEN nri_category IN ('volcano')
                                    THEN LEAST(coalesce(damage_adjusted, 0), VLCN_EXPPE):: double precision / NULLIF (VLCN_EXPPE, 0)
                                WHEN nri_category IN ('coastal')
                                    THEN LEAST(coalesce(damage_adjusted, 0), CFLD_EXPPE):: double precision / NULLIF (CFLD_EXPPE, 0)
                                END, 0)
               END loss_ratio_per_basis
    FROM ${pb_schema}.${pb_table}  pb
             JOIN ${nri_schema}.${nri_table} nri
                  ON pb.geoid = nri.stcofips
                  and ((EXTRACT(YEAR from pb.event_day_date) >= ${startYear}
                        and EXTRACT(YEAR from pb.event_day_date) <= ${endYear})
                        OR event_day_date is null)
),
       national as (
           select ctype,
                  nri_category,
                  avg (loss_ratio_per_basis) av_n,
                  variance(loss_ratio_per_basis) va_n
           from lrpbs as a
           WHERE nri_category is not null
           group by 1, 2
           order by 1
       ),
       regional as (
           select ctype,
                  region,
                  nri_category,
                  count (1),
                  avg (loss_ratio_per_basis) av_r,
                  variance(loss_ratio_per_basis) va_r
           from lrpbs as a
                    join fips_to_regions_and_surrounding_counties as b
                         on b.fips = a.geoid
                             and nri_category != 'hurricane'
           WHERE nri_category is not null
           group by 1, 2, 3

           UNION ALL

           select ctype,
                  region,
                  nri_category,
                  count (1),
                  avg (loss_ratio_per_basis) av_r,
                  variance(loss_ratio_per_basis) va_r
           from lrpbs as a
                    join fips_to_regions_and_surrounding_counties_hurricane as b
                         on b.fips = a.geoid
                             and nri_category = 'hurricane'
           WHERE nri_category is not null
           group by 1, 2, 3
           order by 1, 2, 3

       ),
       county as (
           select ctype,
                  LEFT (b.fips, 5) fips,
                  b.region,
                  nri_category,
                  count (1),
                  avg (loss_ratio_per_basis) av_c,
                  variance(loss_ratio_per_basis) va_c
           from lrpbs as a

                    join fips_to_regions_and_surrounding_counties as b
                         on b.fips = a.geoid
                             and nri_category != 'hurricane'
           WHERE nri_category is not null
           group by 1, 2, 3, 4

           UNION ALL

           select ctype,
                  LEFT (b.fips, 5) fips,
                  b.region,
                  nri_category,
                  count (1),
                  avg (loss_ratio_per_basis) av_c,
                  variance(loss_ratio_per_basis) va_c
           from lrpbs as a
                    join fips_to_regions_and_surrounding_counties_hurricane as b
                         on b.fips = a.geoid
                             and nri_category = 'hurricane'
           WHERE nri_category is not null
           group by 1, 2, 3, 4
           order by 1, 2, 3, 4
       ),
       grid_variance as (
           select grid.id, nri_category, ctype, avg(loss_ratio_per_basis) av, variance(loss_ratio_per_basis) va
           from tmp_fips_to_grid_mapping_196_newer grid
                    JOIN lrpbs
                         ON grid.fips = lrpbs.geoid
           group by 1, 2, 3
           order by 1, 2, 3, 4),
       fips_to_id_ranking as (
           select grid.fips, grid.id, nri_category, ctype, av, va,
                  (covered_area * 100) / total_area percent_area_covered,
                  rank() over (partition by grid.fips, nri_category, ctype order by va, ((covered_area * 100) / total_area) desc ),
                  first_value(grid.id) over (partition by grid.fips, nri_category, ctype order by va, ((covered_area * 100) / total_area) desc ) lowest_var_highest_area_id,
                  first_value(grid.fips) over (partition by grid.id, nri_category, ctype order by va, ((covered_area * 100) / total_area) desc ) lowest_var_geoid
           from tmp_fips_to_grid_mapping_196_newer grid
                    JOIN (select fips, sum(covered_area) total_area from tmp_fips_to_grid_mapping_196_newer group by 1) total_area
                         ON total_area.fips = grid.fips
                    JOIN grid_variance
                         ON grid_variance.id = grid.id
           order by 1, 5, 6 desc),
       fips_to_id_mapping as (
           select distinct fips as geoid, nri_category, ctype, av, va, lowest_var_highest_area_id, lowest_var_geoid
           from fips_to_id_ranking
		   where rank = 1
           order by 1
       ),
       surrounding as (
           select geoid,
            lowest_var_geoid as surrounding, -- correct way is to get grid id, not surrounding county
            nri_category, ctype,
				   lowest_var_highest_area_id,
				   av av_s,
				   va va_s
           from fips_to_id_mapping
		       order by 1, 3, 4
       ),
      geoidSource as (
             SELECT distinct county.geoid,
                  nri_category,
                  ctype,
                  coalesce(h_regions.region, regions.region) region
            FROM geo.tl_2017_county_286 county
            CROSS JOIN (SELECT DISTINCT nri_category FROM lrpbs) cats
            CROSS JOIN (SELECT DISTINCT ctype FROM lrpbs) ctypes
            LEFT JOIN (SELECT fips geoid, region from fips_to_regions_and_surrounding_counties_hurricane) h_regions
              ON county.geoid = h_regions.geoid
                AND cats.nri_category = 'hurricane'
            LEFT JOIN (SELECT fips geoid, region from fips_to_regions_and_surrounding_counties) regions
              ON county.geoid = regions.geoid
                AND cats.nri_category != 'hurricane'
           ),
       hlr as (
           select geoidSource.ctype,
                  geoidSource.geoid,
                  geoidSource.region,
                  surrounding.surrounding surrounding,
                  geoidSource.nri_category,
                  va_n, av_n, va_r, av_r, va_c, av_c, va_s, av_s,
               		   CASE
                      WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat')
                          THEN COALESCE(((
                                                 (1.0 / NULLIF(va_n, 0)) /
                                                 (
                                                         CASE WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat') THEN COALESCE(1.0 / NULLIF(va_n, 0), 0) ELSE 0 END +
                                                         CASE  WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire') THEN COALESCE(1.0 / NULLIF(va_r, 0), 0) ELSE 0 END +
                                                         CASE WHEN geoidSource.nri_category not in ('landslide') THEN COALESCE(1.0 / NULLIF(va_c, 0), 0) ELSE 0 END +
                                                         CASE WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine') THEN COALESCE(1.0 / NULLIF(va_s, 0), 0) ELSE 0 END
                                                     )
                                             )

                                            ), 0)
                      ELSE 0
                      END wt_n,
		            CASE
                      WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat')
                          THEN COALESCE(((
                                                 (1.0 / NULLIF(va_n, 0)) /
                                                 (
                                                         CASE WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat') THEN COALESCE(1.0 / NULLIF(va_n, 0), 0) ELSE 0 END +
                                                         CASE  WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire') THEN COALESCE(1.0 / NULLIF(va_r, 0), 0) ELSE 0 END +
                                                         CASE WHEN geoidSource.nri_category not in ('landslide') THEN COALESCE(1.0 / NULLIF(va_c, 0), 0) ELSE 0 END +
                                                         CASE WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine') THEN COALESCE(1.0 / NULLIF(va_s, 0), 0) ELSE 0 END
                                                     )
                                             ) * av_n

                                            ), 0)
                      ELSE 0
                      END hlr_n,
		            CASE
                      WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire')
                          THEN
                          COALESCE(((
                                            (1.0 / NULLIF(va_r, 0)) /
                                            (
                                                    CASE WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat') THEN COALESCE(1.0 / NULLIF(va_n, 0), 0) ELSE 0 END +
                                                    CASE  WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire') THEN COALESCE(1.0 / NULLIF(va_r, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('landslide') THEN COALESCE(1.0 / NULLIF(va_c, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine') THEN COALESCE(1.0 / NULLIF(va_s, 0), 0) ELSE 0 END
                                                )
                                        ) ), 0)
                      ELSE 0
                      END wt_r,
		            CASE
                      WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire')
                          THEN
                          COALESCE(((
                                            (1.0 / NULLIF(va_r, 0)) /
                                            (
                                                    CASE WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat') THEN COALESCE(1.0 / NULLIF(va_n, 0), 0) ELSE 0 END +
                                                    CASE  WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire') THEN COALESCE(1.0 / NULLIF(va_r, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('landslide') THEN COALESCE(1.0 / NULLIF(va_c, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine') THEN COALESCE(1.0 / NULLIF(va_s, 0), 0) ELSE 0 END
                                                )
                                        ) *
                                    av_r), 0)
                      ELSE 0
                      END hlr_r,
		            CASE
                      WHEN geoidSource.nri_category not in ('landslide')
                          THEN
                          COALESCE(((
                                            (1.0 / NULLIF(va_c, 0)) /
                                            (
                                                    CASE WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat') THEN COALESCE(1.0 / NULLIF(va_n, 0), 0) ELSE 0 END +
                                                    CASE  WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire') THEN COALESCE(1.0 / NULLIF(va_r, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('landslide') THEN COALESCE(1.0 / NULLIF(va_c, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine') THEN COALESCE(1.0 / NULLIF(va_s, 0), 0) ELSE 0 END
                                                )
                                        )), 0)
                      ELSE 0
                      END wt_c,
		            CASE
                      WHEN geoidSource.nri_category not in ('landslide')
                          THEN
                          COALESCE(((
                                            (1.0 / NULLIF(va_c, 0)) /
                                            (
                                                    CASE WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat') THEN COALESCE(1.0 / NULLIF(va_n, 0), 0) ELSE 0 END +
                                                    CASE  WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire') THEN COALESCE(1.0 / NULLIF(va_r, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('landslide') THEN COALESCE(1.0 / NULLIF(va_c, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine') THEN COALESCE(1.0 / NULLIF(va_s, 0), 0) ELSE 0 END
                                                )
                                        ) *
                                    av_c), 0)
                      ELSE 0
                      END hlr_c,
		            CASE
                      WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine')
                          THEN
                          COALESCE(((
                                            (1.0 / NULLIF(va_s, 0)) /
                                            (
                                                    CASE WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat') THEN COALESCE(1.0 / NULLIF(va_n, 0), 0) ELSE 0 END +
                                                    CASE  WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire') THEN COALESCE(1.0 / NULLIF(va_r, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('landslide') THEN COALESCE(1.0 / NULLIF(va_c, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine') THEN COALESCE(1.0 / NULLIF(va_s, 0), 0) ELSE 0 END
                                                )
                                        )), 0)
                      ELSE 0
                      END wt_s,
		            CASE
                      WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine')
                          THEN
                          COALESCE(((
                                            (1.0 / NULLIF(va_s, 0)) /
                                            (
                                                    CASE WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat') THEN COALESCE(1.0 / NULLIF(va_n, 0), 0) ELSE 0 END +
                                                    CASE  WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire') THEN COALESCE(1.0 / NULLIF(va_r, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('landslide') THEN COALESCE(1.0 / NULLIF(va_c, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine') THEN COALESCE(1.0 / NULLIF(va_s, 0), 0) ELSE 0 END
                                                )
                                        ) *
                                    av_s), 0)
                      ELSE 0
                      END hlr_s,
                  CASE
                      WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat')
                          THEN COALESCE(((
                                                 (1.0 / NULLIF(va_n, 0)) /
                                                 (
                                                         CASE WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat') THEN COALESCE(1.0 / NULLIF(va_n, 0), 0) ELSE 0 END +
                                                         CASE  WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire') THEN COALESCE(1.0 / NULLIF(va_r, 0), 0) ELSE 0 END +
                                                         CASE WHEN geoidSource.nri_category not in ('landslide') THEN COALESCE(1.0 / NULLIF(va_c, 0), 0) ELSE 0 END +
                                                         CASE WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine') THEN COALESCE(1.0 / NULLIF(va_s, 0), 0) ELSE 0 END
                                                     )
                                             ) * av_n

                                            ), 0)
                      ELSE 0
                      END
                      +
                  CASE
                      WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire')
                          THEN
                          COALESCE(((
                                            (1.0 / NULLIF(va_r, 0)) /
                                            (
                                                    CASE WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat') THEN COALESCE(1.0 / NULLIF(va_n, 0), 0) ELSE 0 END +
                                                    CASE  WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire') THEN COALESCE(1.0 / NULLIF(va_r, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('landslide') THEN COALESCE(1.0 / NULLIF(va_c, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine') THEN COALESCE(1.0 / NULLIF(va_s, 0), 0) ELSE 0 END
                                                )
                                        ) *
                                    av_r), 0)
                      ELSE 0
                      END
                      +
                  CASE
                      WHEN geoidSource.nri_category not in ('landslide')
                          THEN
                          COALESCE(((
                                            (1.0 / NULLIF(va_c, 0)) /
                                            (
                                                    CASE WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat') THEN COALESCE(1.0 / NULLIF(va_n, 0), 0) ELSE 0 END +
                                                    CASE  WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire') THEN COALESCE(1.0 / NULLIF(va_r, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('landslide') THEN COALESCE(1.0 / NULLIF(va_c, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine') THEN COALESCE(1.0 / NULLIF(va_s, 0), 0) ELSE 0 END
                                                )
                                        ) *
                                    av_c), 0)
                      ELSE 0
                      END
                      +
                  CASE
                      WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine')
                          THEN
                          COALESCE(((
                                            (1.0 / NULLIF(va_s, 0)) /
                                            (
                                                    CASE WHEN geoidSource.nri_category not in ('coastal', 'drought', 'hurricane', 'landslide', 'riverine', 'winterweat') THEN COALESCE(1.0 / NULLIF(va_n, 0), 0) ELSE 0 END +
                                                    CASE  WHEN geoidSource.nri_category not in ('avalanche', 'earthquake', 'landslide', 'lightning', 'volcano', 'wildfire') THEN COALESCE(1.0 / NULLIF(va_r, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('landslide') THEN COALESCE(1.0 / NULLIF(va_c, 0), 0) ELSE 0 END +
                                                    CASE WHEN geoidSource.nri_category not in ('avalanche', 'landslide', 'riverine') THEN COALESCE(1.0 / NULLIF(va_s, 0), 0) ELSE 0 END
                                                )
                                        ) *
                                    av_s), 0)
                      ELSE 0
                      END
                      +
                  CASE
                      WHEN geoidSource.nri_category in ('landslide')
                          THEN
                          COALESCE(av_c, 0)
                      ELSE 0
                      END hlr
          FROM  geoidSource
		   			LEFT JOIN county
		   			ON geoidSource.geoid = county.fips
		   			AND geoidSource.nri_category = county.nri_category
		   			AND geoidSource.ctype = county.ctype
            LEFT JOIN national
                 ON geoidSource.nri_category = national.nri_category
                     AND geoidSource.ctype = national.ctype
            LEFT JOIN regional
                 ON geoidSource.nri_category = regional.nri_category
                     AND geoidSource.region = regional.region
                     AND geoidSource.ctype = regional.ctype
            LEFT JOIN surrounding
                 ON geoidSource.nri_category = surrounding.nri_category
                     AND geoidSource.geoid = surrounding.geoid
                     AND geoidSource.ctype = surrounding.ctype
       )


SELECT * INTO ${pb_schema}.${table_name}_${view_id} FROM hlr
`;
