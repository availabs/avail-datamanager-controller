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
SELECT
               distinct substring(geoid, 1, 5) fips,
                        CASE

                            WHEN substring(geoid, 1, 5) IN ('48215','48061','48489','48261','48047','48249','48273','48355','48409','48391','48057','48321','48039','48167','48071','48245','48361','22023','22113','22045','22101','22109','22057','22075','22087','22071','28157','28005','28113','28147','22117','22103','22009','22079','22115','22011','22019','22053','22003','22039','22001','22097','22055','22099','22047','22005','22093','22095','22089','48297','48025','48175','48469','48239','48481','48157','48201','48291','48199','48241','48351','48457','48373','48407','48339','48473','48015','48089','48285','48123','48255','48477','48185','48471','48455','48313','22077','22125','22037','22091','22105','22063','22033','22121','22007','22051','48007','28091')
                                THEN ('w')

                            WHEN substring(geoid, 1, 5) IN ('48271','48137','48267','48327','48095','48083','48133','48363','48497','48097','48181','48237','48337','40121','40061','40029','40069','40085','40095','40013','40023','40089','40005','40127','40077','40079','40135','48049','48093','48143','48425','48221','48367','48439','48121','48085','48147','48277','48387','48037','48067','48315','48203','48365','48419','48347','48405','48403','48005','48051','48225','48427','48247','48505','48131','48479','48311','48283','48127','48323','48507','48163','48013','48493','48029','48325','48463','48385','48019','48259','48265','48171','48031','48053','48299','48319','48307','48411','48281','48333','48193','48099','48027','48331','48395','48293','48217','48251','48139','48113','48349','48161','48289','48001','48073','48401','48183','48423','48467','48257','48397','48379','48231','48223','48119','48159','48449','48343','48063','48035','48309','48145','48213','48459','48499','48149','48177','48287','48021','48055','48187','48091','48209','48453','48491','48041','22085','22069','22043','22059','22025','22029','22017','22031','22081','22015','22119','22013','22061','22049','22127','22021','22041','22107','22065','22035','22123','22067','22111','22027','22073','22083','05007','05015','05009','05089','05005','05049','05065','05135','05121','05075','05021','05055','05093','05031','05111','05035','05037','05123','05077','05107','05041','05017','05003','05043','05025','05011','05013','05103','05139','05027','05073','05091','05081','05061','05133','05113','05127','05131','05083','05071','05047','05033','05143','05115','05087','05101','05129','05137','05063','05067','05147','05095','05001','05079','05069','05053','05039','05019','05099','05057','05109','05097','05149','05105','05125','05119','05085','05117','05145','05045','05051','05059','05029','05141','05023')
                                THEN ('v')

                            WHEN substring(geoid, 1, 2) IN ('12')
                                THEN ('t')

                            WHEN substring(geoid, 1, 5) IN ('28073','28035','28067','28153','01023','28023','28031','28109','28045','28047','28059','28039','28131','28111','28041','01129','01097','01025','01099','01013','01041','01109','01005','01067','01045','01031','01061','01069','01053','01039','01035','13061','13099','13201','13087','13253','13131','13275','13205','13007','01003','13049','13025','13229','13001','13267','13109','13179','13183','13191','13305','13127','13039','13031','13029','45005','13251','13103','13051','45009','45075','45027','45035','45029','45049','45053','45013','45015','45043','45089','45019','45041','45067','45051','45033','37155','37017','37047','37019','37129','37061','37141','37133','37103','37049','37031','37013','37137','37163','37191','37079','37107','37147','37053','37029','37139','37143','37041','37187','37177','37095','37055','37117','37015','37091','37073','51830')
                                THEN ('t')

                            WHEN substring(geoid, 1, 2) IN ('47')
                                THEN ('s')

                            WHEN substring(geoid, 1, 5) IN ('28125','28055','28149','28001','28037','28085','28077','28065','28063','28029','28127','28129','28061','28075','28021','28049','28121','28123','28101','28069','28099','28079','28089','28163','28053','28007','28083','28133','28151','28011','28027','28135','28119','28143','28137','28033','28093','28009','28139','28003','28117','28141','28057','28081','28145','28115','28071','28161','28013','28017','28095','28025','28155','28097','28051','28015','28043','28107','28019','28105','28087','28103','28159','01077','01083','01089','01071','01079','01059','01033','01093','01075','01107','01125','01073','01127','01057','01009','01115','01015','01043','01029','01111','01017','01081','01123','01037','01117','01121','01027','01133','01103','01095','01055','01049','01019','01005','01113','01011','01087','01101','01051','01001','01021','01007','01105','01065','01063','01085','01131','01047','01091','01119','13239','13243','13273','13177','13261','13307','13053','13197','13249','13193','13269','13263','13145','13285','13199','13231','13171','13149','13045','13143','13233','13115','13055','13083','13295','13047','13313','13213','13111','13123','13129','13215','13259','13101','13185','13027','13075','13173','13069','13155','13277','13019','13321','13287','13017','13315','13081','13093','13235','13023','13153','13293','13079','13225','13037','13007','13095','13205','13071','13061','13065','13005','13161','13299','13003','13175','13309','13271','13091','13163','13167','13303','13125','13301','13189','13073','13133','13221','13265','13141','13009','13319','13289','13169','13021','13207','13159','13237','13211','13217','13151','13255','13113','13063','13089','13121','13077','13097','13223','13067','13015','13057','13085','13227','13187','13291','13281','13241','13035','13247','13297','13219','13059','13135','13013','13157','13195','13105','13147','13119','13011','13139','13117','13311','13137','13257','13033','13245','13283','45011','45003','45037','13181','13317','45065','45001','45047','45081','45063','45079','45071','45059','45007','45073','45077','45045','45083','45021','45091','45023','45057','45017','45085','45069','45031','45025','45055','45039','45087','45061','37165','37153','37007','37179','37093','37051','37085','37125','37123','37167','37025','37101','37195','37065','37083','37127','37069','37185','37181','37077','37063','37135','37001','37081','37067','37057','37151','37037','37183','37105','37131','37145','37033','37157','37169','37171','37005','37009','37189','37011','37121','37115','37021','37111','37023','37035','37097','37159','37059','37003','37027','37161','37071','37119','37109','37149','37089','37175','37087','37193','37197','37045','37173','37199','37113','37043','37039','37075','37099','13165','13107','13043','13279','13209')
                                THEN ('s')

                            WHEN substring(geoid, 1, 2) IN ('54')
                                THEN ('p')

                            WHEN substring(geoid, 1, 5) IN ('34021','51720','51520','51730','51760','24013','24021','24023','24001','51107','24027','24043','51061','51047','51157','51187','51171','51043','51069','51840','51177','51033','51085','51165','51139','51113','51137','51015','51091','51017','51005','51045','51071','51021','51185','51027','51051','51195','51105','51169','51191','51077','51035','51141','51143','51083','51117','51025','51081','51183','51175','51053','51007','51145','51087','51075','51109','51003','51125','51163','51023','51161','51121','51155','51197','51173','51167','51067','51019','51031','51011','51147','51029','51063','51770','51009','51079','51065','51049','51135','51111','51037','51590','51790','51820','51660','51089','51690','51640','51750','51775','51580','51678','51530','51680','51540','51595','42133','42071','42011','42077','42095','42029','42091','42017','34031','34037','34041','34027','34035','34019','36071','36079','36027','09005','09003','09013','09015','25027','33011','33005','33019','33013','33001','33003','33009','23017','33007','23007','23025','23021','23019','23003','23001','23011','42049','42123','42083','42105','42117','42015','42115','42127','42103','42089','42025','42107','42075','42043','42041','42001','42055','42057','42009','42111','42051','42059','42125','42007','42073','42085','42063','42021','42013','42061','42087','42067','42109','42119','42027','42035','42023','42047','42033','42065','42079','42131','42069','42113','42081','42037','42093','42099','42039','42121','42053','42031','42019','42005','42129','42003','42097','36089','36033','36019','36031','36113','36041','36049','36045','36075','36091','36083','36021','36039','36111','36105','36025','36007','36107','36015','36101','36003','36009','36013','36029','36121','36037','36051','36069','36123','36099','36011','36067','36053','36017','36043','36063','36073','36055','36117','36097','36109','36023','36065','36077','36095','36001','36093','36057','36035','36115','50013','50011','50019','50009','50005','50015','50007','50023','50017','50001','50021','50027','50025','50003','25003','25011','25015','25013')
                                THEN ('p')

                            WHEN substring(geoid, 1, 5) IN ('51630','51800','51550','51810','51740','51710','51650','51700','51199','51073','51115','51131','51001','24047','24039','10005','10001','24029','24011','24041','24019','24037','24009','24003','51093','51181','51735','51620','51830','51095','51041','51149','51570','51670','51036','51127','24045','24035','24015','24025','24005','24510','24031','24033','51059','11001','51013','51510','51600','51153','51179','51099','51057','51097','51101','51119','51103','51133','51159','51193','24017','51683','51610','51685','10003','34025','34029','34001','34009','34011','34033','34015','34007','34005','34003','34017','36085','34013','34039','34023','42101','42045','36087','36119','09001','36005','36061','36047','36081','36059','36103','09009','09007','09011','44007','44003','44009','44001','44005','25017','25021','25025','25023','25001','25005','33015','33017','25009','23031','23005','23027','23013','23015','23023','23009','23029','25007','25019')
                                THEN ('q')

                            WHEN substring(geoid, 1, 2) IN ('31','20','19','29','27','55','17','18','21','26','39')
                                THEN ('r')

                            WHEN substring(geoid, 1, 2) IN ('49','08','56','30','38','46')
                                THEN ('u')

                            WHEN substring(geoid, 1, 2) IN ('16','41','53','02')
                                THEN ('z')

                            WHEN substring(geoid, 1, 2) IN ('04','06','32','35','15')
                                THEN ('y')

                            WHEN substring(geoid, 1, 5) IN ('48465','48435','48413','48095','48083','48059','48417','48429','48503','48077','40067','40137','40019','48043','48443','48371','48243','48109','48229','48141','48301','48495','48475','48135','48103','48329','48461','48173','48383','48431','48235','48081','48451','48399','48441','48353','48335','48227','48317','48003','48165','48115','48033','48415','48151','48253','48447','48433','48263','48169','48305','48445','48501','48079','48219','48303','48107','48125','48269','48275','48023','48009','48485','48487','48197','48155','48101','48345','48153','48189','48279','48017','48111','48421','48195','48357','48295','48211','48393','48233','48341','48205','48359','48375','48065','48179','48483','48087','48129','48011','48381','48117','48369','48069','48437','48045','48191','48075','48207','48105','48389','48377','40099','40049','40051','40031','40033','40141','40065','40057','40055','40045','40153','40093','40043','40039','40149','40059','40151','40003','40053','40071','40113','40103','40047','40011','40015','40075','40009','40129','40025','40139','40007','40001','40041','40115','40035','40105','40147','40117','40143','40131','40097','40021','40101','40111','40037','40119','40081','40083','40109','40017','40073','40087','40027','40125','40133','40145','40091','40107','40063','40123')
                                THEN ('y')
                            ELSE 'UNCLASSIFIED'
                            END region
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

-- \tWHERE nri_category = 'hurricane' and geoid = '37013' and event_day_date = '1996-07-12 10:00:00'
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
