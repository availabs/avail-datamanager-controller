# Geospatial Dataset Integrator

The GeospatialDataIntegrator receives a geospatial dataset,
analyzes it, creates a database tables for the dataset layers,
and loads those tables.

During analysis, the integrator generates metadata describing
the dataset as a whole, as well as for requested layers.
Layer analysis is by request only because it reads and analyzes
each feature in the layer and therefore is time consuming.
Descriptive metadata files for the dataset and its layer are immutable.

In addition to the metadata files, a TableDescriptor object
is created for each requested layer. This object contains
mappings between the dataset layer name and the table name,
the dataset field names and the table columns, as well as
finer control configuration options. This object is mutable.
It is initially generated with default values and can be
modified by a user to customize the database table schema.

## NOTES for future work

### Geospatial Dataset Layer Table Descriptor

If either requiresPromoteToMulti or requiresForcedPostGisDimension are true,
and either promoteToMulti or forcePostGisDimension are false,
then the wkb_geometry column type MUST be Geometry. The requires\* fields
are set to true only if nonhomogeneous feature geometries were detected
during layer analysis.

### Staging and Publishing Data Sources

TODO: Implement

When a table is created and loaded, it is given the status STAGED.
A staged data source integrator UI can use API routes that can see STAGED tables.
Normal avail-falcor API routes should not be able to see STAGED tables.
Staging API routes should be able to see STAGED tables to enable QA visualizations.
Once everything looks good, the user can change the table status to PUBLISHED.
STAGED tables can be DROPPED by the UI client. PUBLISHED tables cannot.
