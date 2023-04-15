const geojsonToMapbox = {
	"LineString":"line",
	"MultiLineString":"line",
	"Point":"circle",
	"MultiPoint":"circle",
	"Polygon":"fill",
	"MultiPolygon":"fill"
}

const mbtilesStyles = {
	line: { 
		"type": "line",
        "paint": {
           "line-color": "black",
           "line-width": 1
    		}
    },
    circle: { 
         "type": "circle",
      	"paint": {
        	   "circle-color": "#B42222",
         	"circle-radius": 4
      	}
    },
    fill: {
    	"type": "fill",
      	"paint": {
         	"fill-color": "#0080ff",
         	"fill-opacity": 0.5
      	}
    }
}

export function getStyleFromJsonType(jsonType) {
	return mbtilesStyles[geojsonToMapbox[jsonType] || 'line']
}