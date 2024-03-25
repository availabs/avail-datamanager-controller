/*
  Show us the many-to-many of parcels-to-LCGUs and many-to-many of footprints-to-LCGUs

    Each parcel can be associated with multiple LCGUs.
    Each LCGU can be associated with multiple parcels.

    Each footprint can be associated with multiple LCGUs.
    Each LCGU can be associated with multiple footprints.

    In the lcgu_poly_lineage table, a row exists for EVERY possible (parcel, footprint) pair for the LCGU.

    For example:

        Parcel P₁ partially overlaps parcel P₂.
          There would be three NCGUs:

            NCGU₁: Difference(P₁, P₂)     -- The portion of P₁ that does not overlap P₂
            NCGU₂: Difference(P₂, P₁)     -- The portion of P₂ that does not overlap P₁
            NCGU₃: Intersection(P₁, P₂)   -- The area where P₁ and P₂ overlap.

        For simplicity, assume
          * footprint F₁ is equal to P₁
          * footprint F₂ is equal to P₂

          Then,
            NCGU₁: Difference(F₁, F₂)     -- The portion of F₁ that does not overlap F₂
            NCGU₂: Difference(F₂, F₁)     -- The portion of F₂ that does not overlap F₁
            NCGU₃: Intersection(F₁, F₂)   -- The area where F₁ and F₂ overlap.

      The possible pairings of

          ogc_fid     p_ogc_fid   n_ogc_fid
            NCGU₁        P₁           F₁
            NCGU₂        P₂           F₂
            NCGU₃        P₁           F₁
            NCGU₃        P₁           F₂
            NCGU₃        P₂           F₁
            NCGU₃        P₂           F₂
*/
BEGIN ;

DROP TABLE IF EXISTS :OUTPUT_SCHEMA.lcgu_poly_lineage CASCADE ;
CREATE TABLE :OUTPUT_SCHEMA.lcgu_poly_lineage (
  ogc_fid           INTEGER REFERENCES :OUTPUT_SCHEMA.lcgu_output (ogc_fid),

  p_dama_view_id    INTEGER REFERENCES data_manager.views(view_id),
  p_ogc_fid         INTEGER REFERENCES :PARCELS_TABLE_SCHEMA.:PARCELS_TABLE_NAME (ogc_fid),

  n_dama_view_id    INTEGER REFERENCES data_manager.views(view_id),
  n_ogc_fid         INTEGER REFERENCES :NYS_ITS_FOOTPRINTS_TABLE_SCHEMA.:NYS_ITS_FOOTPRINTS_TABLE_NAME (ogc_fid),

  UNIQUE (ogc_fid, p_ogc_fid, n_ogc_fid)
) WITH (fillfactor=100) ;

INSERT INTO :OUTPUT_SCHEMA.lcgu_poly_lineage (
  ogc_fid,
  p_dama_view_id,
  p_ogc_fid,
  n_dama_view_id,
  n_ogc_fid
)
  SELECT
      ogc_fid,

      p.dama_view_id    AS p_dama_view_id,
      p.union_input_id  AS p_ogc_fid,

      n.dama_view_id    AS n_dama_view_id,
      n.union_input_id  AS n_ogc_fid

    FROM (
        SELECT
            ogc_fid,
            dama_view_id,
            union_input_id
          FROM :OUTPUT_SCHEMA.lcgu_union_input_meta
          WHERE ( dama_view_id = :PARCELS_VIEW_ID )
      ) AS p FULL OUTER JOIN (
        SELECT
            ogc_fid,
            dama_view_id,
            union_input_id
          FROM :OUTPUT_SCHEMA.lcgu_union_input_meta
          WHERE ( dama_view_id = :NYS_ITS_FOOTPRINTS_VIEW_ID )
      ) AS n USING (ogc_fid)
;

CLUSTER :OUTPUT_SCHEMA.lcgu_poly_lineage
  USING lcgu_poly_lineage_ogc_fid_p_ogc_fid_n_ogc_fid_key
;

COMMIT ;
