# Daylight Map Distribution

* https://daylightmap.org/

* [2022: Increasing OpenStreetMap Data Accessibility with the Analysis-Ready Daylight Distribution of OpenStreetMap: A Demonstration of Cloud-Based Assessments of Global Building Completeness](https://www.youtube.com/watch?v=rVBGLCFKAtw&t=161s)
* [2019: “Keepin' it fresh and good!” Continuous Ingestion of OSM Data at Facebook - Saurav Mohapatra](https://www.youtube.com/watch?v=69kSWU-WIGA)
* [2018: OSM at Facebook](https://www.youtube.com/watch?v=Pa05L_vuLZA)

> Daylight is a complete distribution of global, open map data that’s freely
> available with support from community and professional mapmakers. Meta combines
> the work of global contributors to projects like OpenStreetMap with quality and
> consistency checks from Daylight mapping partners to create a free, stable, and
> easy-to-use street-scale global map.
> 
> The Daylight Map Distribution contains a validated subset of the OpenStreetMap
> database. In addition to the standard OpenStreetMap PBF format, Daylight is
> available in two parquet formats that are optimized for AWS Athena including
> geometries (Points, LineStrings, Polygons, or MultiPolygons). First, Daylight
> OSM Features contains the nearly 1B renderable OSM features. Second, Daylight
> OSM Elements contains all of OSM, including all 7B nodes without attributes,
> and relations that do not contain geometries, such as turn restrictions.
> 
> Daylight Earth Table is a new data schema that classifies OpenStreetMap-style
> tags into a 3-level ontology (theme, class, subclass) and is the result of
> running the earth table classification over the latest release (v1.18) of the
> Daylight Map Distribution. The Daylight Earth Table is available as parquet
> files on Amazon S3.

* https://daylightmap.org/2024/01/17/daylight-v138-release.html

# Daylight Earth Table

> The earth table is a new data schema that classifies OpenStreetMap-style tags
> into a 3-level ontology: theme, class, and subclass. A small fishpond, for
> example, would be found in the water theme, pond class, and fishpond subclass.
> A kindergarten school is found in the building theme, education class,
> kindergarten subclass.
> 
> The Daylight Earth Table is the result of running the earth table
> classification over the latest release (v1.35) of the Daylight Map
> Distribution. The daylight earth table is available as parquet files on Amazon
> S3. The instructions on this page will create the table inside your AWS account
> so that you can access the features with Amazon Athena.

* https://daylightmap.org/earth/

## Question: Does the Daylight Map Distributio buildings file include all the Microsoft Footprints and ESRI improvements?

* https://www.openstreetmap.org/user/L%20Freil/diary/394108
* https://www.esri.com/arcgis-blog/products/arcgis-living-atlas/mapping/dawn-of-osm-daylight-in-arcgis/

## Creating the NYS extract

* https://docs.osmcode.org/osmium/latest/osmium-extract.html

```sh
avail@saturn:~/code/avail-gis-toolkit/tasks/daylight_map$ osmium extract --fsync -v -b -79.7619758,40.476578,-71.790972,45.0158611 ml-buildings-v1.38.osm.pbf -o nys.ml-buildings-v1.38.osm.pbf &> extract.log
avail@saturn:~/code/avail-gis-toolkit/tasks/daylight_map$ osmium extract --fsync -v -p new-york-state_us-northeast-220101.poly nys.ml-buildings-v1.38.osm.pbf -o nys-poly.ml-buildings-v1.38.osm.pbf &> poly-extrac
```

## Resources

* https://engineering.fb.com/2019/09/30/ml-applications/mars/

* https://techcrunch.com/2018/08/30/mapbox-vandalism/

* https://www.esri.com/arcgis-blog/products/arcgis-living-atlas/mapping/dawn-of-osm-daylight-in-arcgis/

* https://medium.com/@jen.wnzel/mapping-the-unmapped-a-guide-to-extracting-unofficial-roads-tracks-with-ml-enhanced-osm-daylight-b48bad0513a1
* https://medium.com/@frederic.rodrigo/state-of-the-art-openstreetmap-extraction-synchronization-under-quality-constraints-3d46907c5151
* https://github.com/OvertureMaps/overture-with-daylight
* https://gist.github.com/jenningsanderson/3e42a99dcb8f760038ad8aa47ea38ce8

* https://registry.opendata.aws/daylight-osm/

* https://media.ccc.de/v/state-of-the-map-2022-academic-track-19380-increasing-openstreetmap-data-accessibility-with-the-analysis-ready-daylight-distribution-of-openstreetmap-a-demonstration-of-cloud-based-assessments-of-global-building-completeness


