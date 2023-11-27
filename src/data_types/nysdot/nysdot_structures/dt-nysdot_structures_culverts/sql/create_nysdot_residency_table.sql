/*
 From: https://www.dot.ny.gov/divisions/engineering/structures/repository/manuals/inventory/NYSDOT_inventory_manual_2020.pdf

  ITEM: NYSDOT Residency (Large Culverts)
  NYSDOT
  PROCEDURE:
  For Large Culverts, record the NYSDOT Residency number that is responsible for the maintenance of the
  structure. Note there are no NYSDOT Residencies for Region 11
*/

CREATE TABLE IF NOT EXISTS nysdot_structures.nysdot_residencies (
  region            TEXT NOT NULL,
  residency         TEXT PRIMARY KEY,
  residency_name    TEXT NOT NULL
) ;

INSERT INTO nysdot_structures.nysdot_residencies (
  region,
  residency,
  residency_name
) VALUES 
  ( '01', '114', 'ALBANY RESIDENCY' ),
  ( '01', '124', 'ESSEX RESIDENCY' ),
  ( '01', '134', 'GREENE RESIDENCY' ),
  ( '01', '144', 'RENSSELAER RESIDENCY' ),
  ( '01', '154', 'SARATOGA RESIDENCY' ),
  ( '01', '164', 'SCHENECTADY RESIDENCY' ),
  ( '01', '174', 'WARREN RESIDENCY' ),
  ( '01', '184', 'WASHINGTON RESIDENCY' ),
  ( '02', '224', 'HAMILTON RESIDENCY' ),
  ( '02', '234', 'HERKIMER RESIDENCY' ),
  ( '02', '254', 'FULTON-MONTGOMERY RESIDENCY' ),
  ( '02', '264', 'ONEIDA EAST RESIDENCY' ),
  ( '02', '274', 'ONEIDA WEST MADISON RESIDENCY' ),
  ( '03', '314', 'SENECA-CAYUGA RESIDENCY' ),
  ( '03', '324', 'CORTLAND-TOMPKINS RESIDENCY' ),
  ( '03', '334', 'ONONDAGA EAST RESIDENCY' ),
  ( '03', '344', 'ONONDAGA WEST RESIDENCY' ),
  ( '03', '354', 'OSWEGO RESIDENCY' ),
  ( '04', '414', 'GENESEE-ORLEANS RESIDENCY' ),
  ( '04', '424', 'LIVINGSTON RESIDENCY' ),
  ( '04', '434', 'MONROE EAST RESIDENCY' ),
  ( '04', '444', 'MONROE WEST RESIDENCY' ),
  ( '04', '474', 'WYOMING RESIDENCY' ),
  ( '04', '484', 'WAYNE-ONTARIO RESIDENCY' ),
  ( '05', '514', 'CATTARAUGUS RESIDENCY' ),
  ( '05', '524', 'CHAUTAUQUA RESIDENCY' ),
  ( '05', '534', 'ERIE NORTH RESIDENCY' ),
  ( '05', '544', 'ERIE SOUTH RESIDENCY' ),
  ( '05', '554', 'NIAGARA RESIDENCY' ),
  ( '06', '614', 'ALLEGANY RESIDENCY' ),
  ( '06', '624', 'STEUBEN RESIDENCY' ),
  ( '06', '634', 'CHEMUNG-SCHUYLER-YATES RES' ),
  ( '06', '644', 'ALLEGANY EAST-STEUBEN WEST' ),
  ( '06', '654', 'TIOGA-CHEMUNG EAST RESIDENCY' ),
  ( '07', '714', 'CLINTON RESIDENCY' ),
  ( '07', '724', 'FRANKLIN RESIDENCY' ),
  ( '07', '734', 'JEFFERSON RESIDENCY' ),
  ( '07', '744', 'LEWIS RESIDENCY' ),
  ( '07', '754', 'SAINT LAWRENCE RESIDENCY' ),
  ( '08', '814', 'COLUMBIA RESIDENCY' ),
  ( '08', '824', 'DUTCHESS NORTH RESIDENCY' ),
  ( '08', '834', 'DUTCHESS SOUTH-PUTNAM RES' ),
  ( '08', '844', 'ORANGE EAST RESIDENCY' ),
  ( '08', '854', 'ORANGE WEST RESIDENCY' ),
  ( '08', '864', 'ROCKLAND RESIDENCY' ),
  ( '08', '874', 'ULSTER RESIDENCY' ),
  ( '08', '884', 'WESTCHESTER NORTH RESIDENCY' ),
  ( '08', '894', 'WESTCHESTER SOUTH RESIDENCY' ),
  ( '09', '914', 'BROOME RESIDENCY' ),
  ( '09', '924', 'CHENANGO RESIDENCY' ),
  ( '09', '944', 'DELAWARE SOUTH RESIDENCY' ),
  ( '09', '954', 'OTSEGO RESIDENCY' ),
  ( '09', '964', 'SCHOHARIE-DELAWARE NORTH' ),
  ( '09', '974', 'SULLIVAN RESIDENCY' ),
  ( '09', '984', 'TIOGA RESIDENCY' ),
  ( '10', '014', 'NASSAU NORTH RESIDENCY' ),
  ( '10', '024', 'NASSAU CENTRAL RESIDENCY' ),
  ( '10', '034', 'SUFFOLK EAST RESIDENCY' ),
  ( '10', '044', 'SUFFOLK CENTRAL RESIDENCY' ),
  ( '10', '054', 'SUFFOLK WEST RESIDENCY' ),
  ( '10', '064', 'NASSAU SOUTH RESIDENCY' )
ON CONFLICT DO NOTHING ;
