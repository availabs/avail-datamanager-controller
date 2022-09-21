# node-gdal

[Creating & writing new datasets](https://github.com/naturalatlas/node-gdal/issues/199)

```js
const inputDataset = gdal.open("./inputFile.shp");
const inputLayer = inputDataset.executeSQL(
  "SELECT CAST(isActive AS boolean) AS active) FROM inputFile"
);
const outputDataset = gdal.open("./outputFile.geojson", "w", "GeoJSON");
const outputLayer = outputDataset.layers.create(
  "myTableName",
  inputLayer.srs,
  inputLayer.geomType
);

// set up output schema
inputLayer.fields.forEach((field) =>
  outputLayer.fields.add(new gdal.FieldDefn(field.name, field.type))
);

inputLayer.features.forEach((feature) => {
  // create feature specifically for the output layer
  const outputFeature = new gdal.Feature(outputLayer);
  outputFeature.setGeometry(feature.getGeometry());
  outputFeature.fields.set(feature.fields.toObject());
  outputLayer.features.add(outputFeature);
});

// outputLayer.flush() - this is unnecessary because the dataset gets flushed when it is closed
outputDataset.close();
```
