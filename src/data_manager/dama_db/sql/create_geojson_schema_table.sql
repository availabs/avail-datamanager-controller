/*
    JSON Schema definitions copied from https://github.com/geojson/schema
*/

CREATE SCHEMA IF NOT EXISTS _data_manager_admin ;

CREATE TABLE IF NOT EXISTS _data_manager_admin.geojson_json_schemas (
    geojson_type        TEXT PRIMARY KEY,
    json_schema         JSONB NOT NULL,

    CONSTRAINT geojson_type_check
      CHECK (
        geojson_type IN (
          'FeatureCollection',
          'Feature',
          'Geometry',
          'GeometryCollection',
          'MultiPolygon',
          'MultiLineString',
          'MultiPoint',
          'Polygon',
          'LineString',
          'Point'
        )
      )
  ) WITH(fillfactor=100)
;

INSERT INTO _data_manager_admin.geojson_json_schemas (
  geojson_type,
  json_schema
)
  VALUES
    (
      'FeatureCollection',
      '
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "$id": "https://geojson.org/schema/FeatureCollection.json",
          "title": "GeoJSON FeatureCollection",
          "type": "object",
          "required": [
            "type",
            "features"
          ],
          "properties": {
            "type": {
              "type": "string",
              "enum": [
                "FeatureCollection"
              ]
            },
            "features": {
              "type": "array",
              "items": {
                "title": "GeoJSON Feature",
                "type": "object",
                "required": [
                  "type",
                  "properties",
                  "geometry"
                ],
                "properties": {
                  "type": {
                    "type": "string",
                    "enum": [
                      "Feature"
                    ]
                  },
                  "id": {
                    "oneOf": [
                      {
                        "type": "number"
                      },
                      {
                        "type": "string"
                      }
                    ]
                  },
                  "properties": {
                    "oneOf": [
                      {
                        "type": "null"
                      },
                      {
                        "type": "object"
                      }
                    ]
                  },
                  "geometry": {
                    "oneOf": [
                      {
                        "type": "null"
                      },
                      {
                        "title": "GeoJSON Point",
                        "type": "object",
                        "required": [
                          "type",
                          "coordinates"
                        ],
                        "properties": {
                          "type": {
                            "type": "string",
                            "enum": [
                              "Point"
                            ]
                          },
                          "coordinates": {
                            "type": "array",
                            "minItems": 2,
                            "items": {
                              "type": "number"
                            }
                          },
                          "bbox": {
                            "type": "array",
                            "minItems": 4,
                            "items": {
                              "type": "number"
                            }
                          }
                        }
                      },
                      {
                        "title": "GeoJSON LineString",
                        "type": "object",
                        "required": [
                          "type",
                          "coordinates"
                        ],
                        "properties": {
                          "type": {
                            "type": "string",
                            "enum": [
                              "LineString"
                            ]
                          },
                          "coordinates": {
                            "type": "array",
                            "minItems": 2,
                            "items": {
                              "type": "array",
                              "minItems": 2,
                              "items": {
                                "type": "number"
                              }
                            }
                          },
                          "bbox": {
                            "type": "array",
                            "minItems": 4,
                            "items": {
                              "type": "number"
                            }
                          }
                        }
                      },
                      {
                        "title": "GeoJSON Polygon",
                        "type": "object",
                        "required": [
                          "type",
                          "coordinates"
                        ],
                        "properties": {
                          "type": {
                            "type": "string",
                            "enum": [
                              "Polygon"
                            ]
                          },
                          "coordinates": {
                            "type": "array",
                            "items": {
                              "type": "array",
                              "minItems": 4,
                              "items": {
                                "type": "array",
                                "minItems": 2,
                                "items": {
                                  "type": "number"
                                }
                              }
                            }
                          },
                          "bbox": {
                            "type": "array",
                            "minItems": 4,
                            "items": {
                              "type": "number"
                            }
                          }
                        }
                      },
                      {
                        "title": "GeoJSON MultiPoint",
                        "type": "object",
                        "required": [
                          "type",
                          "coordinates"
                        ],
                        "properties": {
                          "type": {
                            "type": "string",
                            "enum": [
                              "MultiPoint"
                            ]
                          },
                          "coordinates": {
                            "type": "array",
                            "items": {
                              "type": "array",
                              "minItems": 2,
                              "items": {
                                "type": "number"
                              }
                            }
                          },
                          "bbox": {
                            "type": "array",
                            "minItems": 4,
                            "items": {
                              "type": "number"
                            }
                          }
                        }
                      },
                      {
                        "title": "GeoJSON MultiLineString",
                        "type": "object",
                        "required": [
                          "type",
                          "coordinates"
                        ],
                        "properties": {
                          "type": {
                            "type": "string",
                            "enum": [
                              "MultiLineString"
                            ]
                          },
                          "coordinates": {
                            "type": "array",
                            "items": {
                              "type": "array",
                              "minItems": 2,
                              "items": {
                                "type": "array",
                                "minItems": 2,
                                "items": {
                                  "type": "number"
                                }
                              }
                            }
                          },
                          "bbox": {
                            "type": "array",
                            "minItems": 4,
                            "items": {
                              "type": "number"
                            }
                          }
                        }
                      },
                      {
                        "title": "GeoJSON MultiPolygon",
                        "type": "object",
                        "required": [
                          "type",
                          "coordinates"
                        ],
                        "properties": {
                          "type": {
                            "type": "string",
                            "enum": [
                              "MultiPolygon"
                            ]
                          },
                          "coordinates": {
                            "type": "array",
                            "items": {
                              "type": "array",
                              "items": {
                                "type": "array",
                                "minItems": 4,
                                "items": {
                                  "type": "array",
                                  "minItems": 2,
                                  "items": {
                                    "type": "number"
                                  }
                                }
                              }
                            }
                          },
                          "bbox": {
                            "type": "array",
                            "minItems": 4,
                            "items": {
                              "type": "number"
                            }
                          }
                        }
                      },
                      {
                        "title": "GeoJSON GeometryCollection",
                        "type": "object",
                        "required": [
                          "type",
                          "geometries"
                        ],
                        "properties": {
                          "type": {
                            "type": "string",
                            "enum": [
                              "GeometryCollection"
                            ]
                          },
                          "geometries": {
                            "type": "array",
                            "items": {
                              "oneOf": [
                                {
                                  "title": "GeoJSON Point",
                                  "type": "object",
                                  "required": [
                                    "type",
                                    "coordinates"
                                  ],
                                  "properties": {
                                    "type": {
                                      "type": "string",
                                      "enum": [
                                        "Point"
                                      ]
                                    },
                                    "coordinates": {
                                      "type": "array",
                                      "minItems": 2,
                                      "items": {
                                        "type": "number"
                                      }
                                    },
                                    "bbox": {
                                      "type": "array",
                                      "minItems": 4,
                                      "items": {
                                        "type": "number"
                                      }
                                    }
                                  }
                                },
                                {
                                  "title": "GeoJSON LineString",
                                  "type": "object",
                                  "required": [
                                    "type",
                                    "coordinates"
                                  ],
                                  "properties": {
                                    "type": {
                                      "type": "string",
                                      "enum": [
                                        "LineString"
                                      ]
                                    },
                                    "coordinates": {
                                      "type": "array",
                                      "minItems": 2,
                                      "items": {
                                        "type": "array",
                                        "minItems": 2,
                                        "items": {
                                          "type": "number"
                                        }
                                      }
                                    },
                                    "bbox": {
                                      "type": "array",
                                      "minItems": 4,
                                      "items": {
                                        "type": "number"
                                      }
                                    }
                                  }
                                },
                                {
                                  "title": "GeoJSON Polygon",
                                  "type": "object",
                                  "required": [
                                    "type",
                                    "coordinates"
                                  ],
                                  "properties": {
                                    "type": {
                                      "type": "string",
                                      "enum": [
                                        "Polygon"
                                      ]
                                    },
                                    "coordinates": {
                                      "type": "array",
                                      "items": {
                                        "type": "array",
                                        "minItems": 4,
                                        "items": {
                                          "type": "array",
                                          "minItems": 2,
                                          "items": {
                                            "type": "number"
                                          }
                                        }
                                      }
                                    },
                                    "bbox": {
                                      "type": "array",
                                      "minItems": 4,
                                      "items": {
                                        "type": "number"
                                      }
                                    }
                                  }
                                },
                                {
                                  "title": "GeoJSON MultiPoint",
                                  "type": "object",
                                  "required": [
                                    "type",
                                    "coordinates"
                                  ],
                                  "properties": {
                                    "type": {
                                      "type": "string",
                                      "enum": [
                                        "MultiPoint"
                                      ]
                                    },
                                    "coordinates": {
                                      "type": "array",
                                      "items": {
                                        "type": "array",
                                        "minItems": 2,
                                        "items": {
                                          "type": "number"
                                        }
                                      }
                                    },
                                    "bbox": {
                                      "type": "array",
                                      "minItems": 4,
                                      "items": {
                                        "type": "number"
                                      }
                                    }
                                  }
                                },
                                {
                                  "title": "GeoJSON MultiLineString",
                                  "type": "object",
                                  "required": [
                                    "type",
                                    "coordinates"
                                  ],
                                  "properties": {
                                    "type": {
                                      "type": "string",
                                      "enum": [
                                        "MultiLineString"
                                      ]
                                    },
                                    "coordinates": {
                                      "type": "array",
                                      "items": {
                                        "type": "array",
                                        "minItems": 2,
                                        "items": {
                                          "type": "array",
                                          "minItems": 2,
                                          "items": {
                                            "type": "number"
                                          }
                                        }
                                      }
                                    },
                                    "bbox": {
                                      "type": "array",
                                      "minItems": 4,
                                      "items": {
                                        "type": "number"
                                      }
                                    }
                                  }
                                },
                                {
                                  "title": "GeoJSON MultiPolygon",
                                  "type": "object",
                                  "required": [
                                    "type",
                                    "coordinates"
                                  ],
                                  "properties": {
                                    "type": {
                                      "type": "string",
                                      "enum": [
                                        "MultiPolygon"
                                      ]
                                    },
                                    "coordinates": {
                                      "type": "array",
                                      "items": {
                                        "type": "array",
                                        "items": {
                                          "type": "array",
                                          "minItems": 4,
                                          "items": {
                                            "type": "array",
                                            "minItems": 2,
                                            "items": {
                                              "type": "number"
                                            }
                                          }
                                        }
                                      }
                                    },
                                    "bbox": {
                                      "type": "array",
                                      "minItems": 4,
                                      "items": {
                                        "type": "number"
                                      }
                                    }
                                  }
                                }
                              ]
                            }
                          },
                          "bbox": {
                            "type": "array",
                            "minItems": 4,
                            "items": {
                              "type": "number"
                            }
                          }
                        }
                      }
                    ]
                  },
                  "bbox": {
                    "type": "array",
                    "minItems": 4,
                    "items": {
                      "type": "number"
                    }
                  }
                }
              }
            },
            "bbox": {
              "type": "array",
              "minItems": 4,
              "items": {
                "type": "number"
              }
            }
          }
        }
      '::JSONB
    ),

    (
      'Feature',
      '
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "$id": "https://geojson.org/schema/Feature.json",
          "title": "GeoJSON Feature",
          "type": "object",
          "required": [
            "type",
            "properties",
            "geometry"
          ],
          "properties": {
            "type": {
              "type": "string",
              "enum": [
                "Feature"
              ]
            },
            "id": {
              "oneOf": [
                {
                  "type": "number"
                },
                {
                  "type": "string"
                }
              ]
            },
            "properties": {
              "oneOf": [
                {
                  "type": "null"
                },
                {
                  "type": "object"
                }
              ]
            },
            "geometry": {
              "oneOf": [
                {
                  "type": "null"
                },
                {
                  "title": "GeoJSON Point",
                  "type": "object",
                  "required": [
                    "type",
                    "coordinates"
                  ],
                  "properties": {
                    "type": {
                      "type": "string",
                      "enum": [
                        "Point"
                      ]
                    },
                    "coordinates": {
                      "type": "array",
                      "minItems": 2,
                      "items": {
                        "type": "number"
                      }
                    },
                    "bbox": {
                      "type": "array",
                      "minItems": 4,
                      "items": {
                        "type": "number"
                      }
                    }
                  }
                },
                {
                  "title": "GeoJSON LineString",
                  "type": "object",
                  "required": [
                    "type",
                    "coordinates"
                  ],
                  "properties": {
                    "type": {
                      "type": "string",
                      "enum": [
                        "LineString"
                      ]
                    },
                    "coordinates": {
                      "type": "array",
                      "minItems": 2,
                      "items": {
                        "type": "array",
                        "minItems": 2,
                        "items": {
                          "type": "number"
                        }
                      }
                    },
                    "bbox": {
                      "type": "array",
                      "minItems": 4,
                      "items": {
                        "type": "number"
                      }
                    }
                  }
                },
                {
                  "title": "GeoJSON Polygon",
                  "type": "object",
                  "required": [
                    "type",
                    "coordinates"
                  ],
                  "properties": {
                    "type": {
                      "type": "string",
                      "enum": [
                        "Polygon"
                      ]
                    },
                    "coordinates": {
                      "type": "array",
                      "items": {
                        "type": "array",
                        "minItems": 4,
                        "items": {
                          "type": "array",
                          "minItems": 2,
                          "items": {
                            "type": "number"
                          }
                        }
                      }
                    },
                    "bbox": {
                      "type": "array",
                      "minItems": 4,
                      "items": {
                        "type": "number"
                      }
                    }
                  }
                },
                {
                  "title": "GeoJSON MultiPoint",
                  "type": "object",
                  "required": [
                    "type",
                    "coordinates"
                  ],
                  "properties": {
                    "type": {
                      "type": "string",
                      "enum": [
                        "MultiPoint"
                      ]
                    },
                    "coordinates": {
                      "type": "array",
                      "items": {
                        "type": "array",
                        "minItems": 2,
                        "items": {
                          "type": "number"
                        }
                      }
                    },
                    "bbox": {
                      "type": "array",
                      "minItems": 4,
                      "items": {
                        "type": "number"
                      }
                    }
                  }
                },
                {
                  "title": "GeoJSON MultiLineString",
                  "type": "object",
                  "required": [
                    "type",
                    "coordinates"
                  ],
                  "properties": {
                    "type": {
                      "type": "string",
                      "enum": [
                        "MultiLineString"
                      ]
                    },
                    "coordinates": {
                      "type": "array",
                      "items": {
                        "type": "array",
                        "minItems": 2,
                        "items": {
                          "type": "array",
                          "minItems": 2,
                          "items": {
                            "type": "number"
                          }
                        }
                      }
                    },
                    "bbox": {
                      "type": "array",
                      "minItems": 4,
                      "items": {
                        "type": "number"
                      }
                    }
                  }
                },
                {
                  "title": "GeoJSON MultiPolygon",
                  "type": "object",
                  "required": [
                    "type",
                    "coordinates"
                  ],
                  "properties": {
                    "type": {
                      "type": "string",
                      "enum": [
                        "MultiPolygon"
                      ]
                    },
                    "coordinates": {
                      "type": "array",
                      "items": {
                        "type": "array",
                        "items": {
                          "type": "array",
                          "minItems": 4,
                          "items": {
                            "type": "array",
                            "minItems": 2,
                            "items": {
                              "type": "number"
                            }
                          }
                        }
                      }
                    },
                    "bbox": {
                      "type": "array",
                      "minItems": 4,
                      "items": {
                        "type": "number"
                      }
                    }
                  }
                },
                {
                  "title": "GeoJSON GeometryCollection",
                  "type": "object",
                  "required": [
                    "type",
                    "geometries"
                  ],
                  "properties": {
                    "type": {
                      "type": "string",
                      "enum": [
                        "GeometryCollection"
                      ]
                    },
                    "geometries": {
                      "type": "array",
                      "items": {
                        "oneOf": [
                          {
                            "title": "GeoJSON Point",
                            "type": "object",
                            "required": [
                              "type",
                              "coordinates"
                            ],
                            "properties": {
                              "type": {
                                "type": "string",
                                "enum": [
                                  "Point"
                                ]
                              },
                              "coordinates": {
                                "type": "array",
                                "minItems": 2,
                                "items": {
                                  "type": "number"
                                }
                              },
                              "bbox": {
                                "type": "array",
                                "minItems": 4,
                                "items": {
                                  "type": "number"
                                }
                              }
                            }
                          },
                          {
                            "title": "GeoJSON LineString",
                            "type": "object",
                            "required": [
                              "type",
                              "coordinates"
                            ],
                            "properties": {
                              "type": {
                                "type": "string",
                                "enum": [
                                  "LineString"
                                ]
                              },
                              "coordinates": {
                                "type": "array",
                                "minItems": 2,
                                "items": {
                                  "type": "array",
                                  "minItems": 2,
                                  "items": {
                                    "type": "number"
                                  }
                                }
                              },
                              "bbox": {
                                "type": "array",
                                "minItems": 4,
                                "items": {
                                  "type": "number"
                                }
                              }
                            }
                          },
                          {
                            "title": "GeoJSON Polygon",
                            "type": "object",
                            "required": [
                              "type",
                              "coordinates"
                            ],
                            "properties": {
                              "type": {
                                "type": "string",
                                "enum": [
                                  "Polygon"
                                ]
                              },
                              "coordinates": {
                                "type": "array",
                                "items": {
                                  "type": "array",
                                  "minItems": 4,
                                  "items": {
                                    "type": "array",
                                    "minItems": 2,
                                    "items": {
                                      "type": "number"
                                    }
                                  }
                                }
                              },
                              "bbox": {
                                "type": "array",
                                "minItems": 4,
                                "items": {
                                  "type": "number"
                                }
                              }
                            }
                          },
                          {
                            "title": "GeoJSON MultiPoint",
                            "type": "object",
                            "required": [
                              "type",
                              "coordinates"
                            ],
                            "properties": {
                              "type": {
                                "type": "string",
                                "enum": [
                                  "MultiPoint"
                                ]
                              },
                              "coordinates": {
                                "type": "array",
                                "items": {
                                  "type": "array",
                                  "minItems": 2,
                                  "items": {
                                    "type": "number"
                                  }
                                }
                              },
                              "bbox": {
                                "type": "array",
                                "minItems": 4,
                                "items": {
                                  "type": "number"
                                }
                              }
                            }
                          },
                          {
                            "title": "GeoJSON MultiLineString",
                            "type": "object",
                            "required": [
                              "type",
                              "coordinates"
                            ],
                            "properties": {
                              "type": {
                                "type": "string",
                                "enum": [
                                  "MultiLineString"
                                ]
                              },
                              "coordinates": {
                                "type": "array",
                                "items": {
                                  "type": "array",
                                  "minItems": 2,
                                  "items": {
                                    "type": "array",
                                    "minItems": 2,
                                    "items": {
                                      "type": "number"
                                    }
                                  }
                                }
                              },
                              "bbox": {
                                "type": "array",
                                "minItems": 4,
                                "items": {
                                  "type": "number"
                                }
                              }
                            }
                          },
                          {
                            "title": "GeoJSON MultiPolygon",
                            "type": "object",
                            "required": [
                              "type",
                              "coordinates"
                            ],
                            "properties": {
                              "type": {
                                "type": "string",
                                "enum": [
                                  "MultiPolygon"
                                ]
                              },
                              "coordinates": {
                                "type": "array",
                                "items": {
                                  "type": "array",
                                  "items": {
                                    "type": "array",
                                    "minItems": 4,
                                    "items": {
                                      "type": "array",
                                      "minItems": 2,
                                      "items": {
                                        "type": "number"
                                      }
                                    }
                                  }
                                }
                              },
                              "bbox": {
                                "type": "array",
                                "minItems": 4,
                                "items": {
                                  "type": "number"
                                }
                              }
                            }
                          }
                        ]
                      }
                    },
                    "bbox": {
                      "type": "array",
                      "minItems": 4,
                      "items": {
                        "type": "number"
                      }
                    }
                  }
                }
              ]
            },
            "bbox": {
              "type": "array",
              "minItems": 4,
              "items": {
                "type": "number"
              }
            }
          }
        }
      '::JSONB
    ),

    (
      'Geometry',
      '
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "$id": "https://geojson.org/schema/Geometry.json",
          "title": "GeoJSON Geometry",
          "oneOf": [
            {
              "title": "GeoJSON Point",
              "type": "object",
              "required": [
                "type",
                "coordinates"
              ],
              "properties": {
                "type": {
                  "type": "string",
                  "enum": [
                    "Point"
                  ]
                },
                "coordinates": {
                  "type": "array",
                  "minItems": 2,
                  "items": {
                    "type": "number"
                  }
                },
                "bbox": {
                  "type": "array",
                  "minItems": 4,
                  "items": {
                    "type": "number"
                  }
                }
              }
            },
            {
              "title": "GeoJSON LineString",
              "type": "object",
              "required": [
                "type",
                "coordinates"
              ],
              "properties": {
                "type": {
                  "type": "string",
                  "enum": [
                    "LineString"
                  ]
                },
                "coordinates": {
                  "type": "array",
                  "minItems": 2,
                  "items": {
                    "type": "array",
                    "minItems": 2,
                    "items": {
                      "type": "number"
                    }
                  }
                },
                "bbox": {
                  "type": "array",
                  "minItems": 4,
                  "items": {
                    "type": "number"
                  }
                }
              }
            },
            {
              "title": "GeoJSON Polygon",
              "type": "object",
              "required": [
                "type",
                "coordinates"
              ],
              "properties": {
                "type": {
                  "type": "string",
                  "enum": [
                    "Polygon"
                  ]
                },
                "coordinates": {
                  "type": "array",
                  "items": {
                    "type": "array",
                    "minItems": 4,
                    "items": {
                      "type": "array",
                      "minItems": 2,
                      "items": {
                        "type": "number"
                      }
                    }
                  }
                },
                "bbox": {
                  "type": "array",
                  "minItems": 4,
                  "items": {
                    "type": "number"
                  }
                }
              }
            },
            {
              "title": "GeoJSON MultiPoint",
              "type": "object",
              "required": [
                "type",
                "coordinates"
              ],
              "properties": {
                "type": {
                  "type": "string",
                  "enum": [
                    "MultiPoint"
                  ]
                },
                "coordinates": {
                  "type": "array",
                  "items": {
                    "type": "array",
                    "minItems": 2,
                    "items": {
                      "type": "number"
                    }
                  }
                },
                "bbox": {
                  "type": "array",
                  "minItems": 4,
                  "items": {
                    "type": "number"
                  }
                }
              }
            },
            {
              "title": "GeoJSON MultiLineString",
              "type": "object",
              "required": [
                "type",
                "coordinates"
              ],
              "properties": {
                "type": {
                  "type": "string",
                  "enum": [
                    "MultiLineString"
                  ]
                },
                "coordinates": {
                  "type": "array",
                  "items": {
                    "type": "array",
                    "minItems": 2,
                    "items": {
                      "type": "array",
                      "minItems": 2,
                      "items": {
                        "type": "number"
                      }
                    }
                  }
                },
                "bbox": {
                  "type": "array",
                  "minItems": 4,
                  "items": {
                    "type": "number"
                  }
                }
              }
            },
            {
              "title": "GeoJSON MultiPolygon",
              "type": "object",
              "required": [
                "type",
                "coordinates"
              ],
              "properties": {
                "type": {
                  "type": "string",
                  "enum": [
                    "MultiPolygon"
                  ]
                },
                "coordinates": {
                  "type": "array",
                  "items": {
                    "type": "array",
                    "items": {
                      "type": "array",
                      "minItems": 4,
                      "items": {
                        "type": "array",
                        "minItems": 2,
                        "items": {
                          "type": "number"
                        }
                      }
                    }
                  }
                },
                "bbox": {
                  "type": "array",
                  "minItems": 4,
                  "items": {
                    "type": "number"
                  }
                }
              }
            }
          ]
        }
      '::JSONB
    ),

    (
      'GeometryCollection',
      '
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "$id": "https://geojson.org/schema/GeometryCollection.json",
          "title": "GeoJSON GeometryCollection",
          "type": "object",
          "required": [
            "type",
            "geometries"
          ],
          "properties": {
            "type": {
              "type": "string",
              "enum": [
                "GeometryCollection"
              ]
            },
            "geometries": {
              "type": "array",
              "items": {
                "oneOf": [
                  {
                    "title": "GeoJSON Point",
                    "type": "object",
                    "required": [
                      "type",
                      "coordinates"
                    ],
                    "properties": {
                      "type": {
                        "type": "string",
                        "enum": [
                          "Point"
                        ]
                      },
                      "coordinates": {
                        "type": "array",
                        "minItems": 2,
                        "items": {
                          "type": "number"
                        }
                      },
                      "bbox": {
                        "type": "array",
                        "minItems": 4,
                        "items": {
                          "type": "number"
                        }
                      }
                    }
                  },
                  {
                    "title": "GeoJSON LineString",
                    "type": "object",
                    "required": [
                      "type",
                      "coordinates"
                    ],
                    "properties": {
                      "type": {
                        "type": "string",
                        "enum": [
                          "LineString"
                        ]
                      },
                      "coordinates": {
                        "type": "array",
                        "minItems": 2,
                        "items": {
                          "type": "array",
                          "minItems": 2,
                          "items": {
                            "type": "number"
                          }
                        }
                      },
                      "bbox": {
                        "type": "array",
                        "minItems": 4,
                        "items": {
                          "type": "number"
                        }
                      }
                    }
                  },
                  {
                    "title": "GeoJSON Polygon",
                    "type": "object",
                    "required": [
                      "type",
                      "coordinates"
                    ],
                    "properties": {
                      "type": {
                        "type": "string",
                        "enum": [
                          "Polygon"
                        ]
                      },
                      "coordinates": {
                        "type": "array",
                        "items": {
                          "type": "array",
                          "minItems": 4,
                          "items": {
                            "type": "array",
                            "minItems": 2,
                            "items": {
                              "type": "number"
                            }
                          }
                        }
                      },
                      "bbox": {
                        "type": "array",
                        "minItems": 4,
                        "items": {
                          "type": "number"
                        }
                      }
                    }
                  },
                  {
                    "title": "GeoJSON MultiPoint",
                    "type": "object",
                    "required": [
                      "type",
                      "coordinates"
                    ],
                    "properties": {
                      "type": {
                        "type": "string",
                        "enum": [
                          "MultiPoint"
                        ]
                      },
                      "coordinates": {
                        "type": "array",
                        "items": {
                          "type": "array",
                          "minItems": 2,
                          "items": {
                            "type": "number"
                          }
                        }
                      },
                      "bbox": {
                        "type": "array",
                        "minItems": 4,
                        "items": {
                          "type": "number"
                        }
                      }
                    }
                  },
                  {
                    "title": "GeoJSON MultiLineString",
                    "type": "object",
                    "required": [
                      "type",
                      "coordinates"
                    ],
                    "properties": {
                      "type": {
                        "type": "string",
                        "enum": [
                          "MultiLineString"
                        ]
                      },
                      "coordinates": {
                        "type": "array",
                        "items": {
                          "type": "array",
                          "minItems": 2,
                          "items": {
                            "type": "array",
                            "minItems": 2,
                            "items": {
                              "type": "number"
                            }
                          }
                        }
                      },
                      "bbox": {
                        "type": "array",
                        "minItems": 4,
                        "items": {
                          "type": "number"
                        }
                      }
                    }
                  },
                  {
                    "title": "GeoJSON MultiPolygon",
                    "type": "object",
                    "required": [
                      "type",
                      "coordinates"
                    ],
                    "properties": {
                      "type": {
                        "type": "string",
                        "enum": [
                          "MultiPolygon"
                        ]
                      },
                      "coordinates": {
                        "type": "array",
                        "items": {
                          "type": "array",
                          "items": {
                            "type": "array",
                            "minItems": 4,
                            "items": {
                              "type": "array",
                              "minItems": 2,
                              "items": {
                                "type": "number"
                              }
                            }
                          }
                        }
                      },
                      "bbox": {
                        "type": "array",
                        "minItems": 4,
                        "items": {
                          "type": "number"
                        }
                      }
                    }
                  }
                ]
              }
            },
            "bbox": {
              "type": "array",
              "minItems": 4,
              "items": {
                "type": "number"
              }
            }
          }
        }
      '::JSONB
    ),

    (
      'MultiPolygon',
      '
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "$id": "https://geojson.org/schema/MultiPolygon.json",
          "title": "GeoJSON MultiPolygon",
          "type": "object",
          "required": [
            "type",
            "coordinates"
          ],
          "properties": {
            "type": {
              "type": "string",
              "enum": [
                "MultiPolygon"
              ]
            },
            "coordinates": {
              "type": "array",
              "items": {
                "type": "array",
                "items": {
                  "type": "array",
                  "minItems": 4,
                  "items": {
                    "type": "array",
                    "minItems": 2,
                    "items": {
                      "type": "number"
                    }
                  }
                }
              }
            },
            "bbox": {
              "type": "array",
              "minItems": 4,
              "items": {
                "type": "number"
              }
            }
          }
        }
      '::JSONB
    ),

    (
      'MultiLineString',
      '
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "$id": "https://geojson.org/schema/MultiLineString.json",
          "title": "GeoJSON MultiLineString",
          "type": "object",
          "required": [
            "type",
            "coordinates"
          ],
          "properties": {
            "type": {
              "type": "string",
              "enum": [
                "MultiLineString"
              ]
            },
            "coordinates": {
              "type": "array",
              "items": {
                "type": "array",
                "minItems": 2,
                "items": {
                  "type": "array",
                  "minItems": 2,
                  "items": {
                    "type": "number"
                  }
                }
              }
            },
            "bbox": {
              "type": "array",
              "minItems": 4,
              "items": {
                "type": "number"
              }
            }
          }
        }
      '::JSONB
    ),

    (
      'MultiPoint',
      '
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "$id": "https://geojson.org/schema/MultiPoint.json",
          "title": "GeoJSON MultiPoint",
          "type": "object",
          "required": [
            "type",
            "coordinates"
          ],
          "properties": {
            "type": {
              "type": "string",
              "enum": [
                "MultiPoint"
              ]
            },
            "coordinates": {
              "type": "array",
              "items": {
                "type": "array",
                "minItems": 2,
                "items": {
                  "type": "number"
                }
              }
            },
            "bbox": {
              "type": "array",
              "minItems": 4,
              "items": {
                "type": "number"
              }
            }
          }
        }
      '::JSONB
    ),

    (
      'Polygon',
      '
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "$id": "https://geojson.org/schema/Polygon.json",
          "title": "GeoJSON Polygon",
          "type": "object",
          "required": [
            "type",
            "coordinates"
          ],
          "properties": {
            "type": {
              "type": "string",
              "enum": [
                "Polygon"
              ]
            },
            "coordinates": {
              "type": "array",
              "items": {
                "type": "array",
                "minItems": 4,
                "items": {
                  "type": "array",
                  "minItems": 2,
                  "items": {
                    "type": "number"
                  }
                }
              }
            },
            "bbox": {
              "type": "array",
              "minItems": 4,
              "items": {
                "type": "number"
              }
            }
          }
        }
      '::JSONB
    ),

    (
      'LineString',
      '
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "$id": "https://geojson.org/schema/LineString.json",
          "title": "GeoJSON LineString",
          "type": "object",
          "required": [
            "type",
            "coordinates"
          ],
          "properties": {
            "type": {
              "type": "string",
              "enum": [
                "LineString"
              ]
            },
            "coordinates": {
              "type": "array",
              "minItems": 2,
              "items": {
                "type": "array",
                "minItems": 2,
                "items": {
                  "type": "number"
                }
              }
            },
            "bbox": {
              "type": "array",
              "minItems": 4,
              "items": {
                "type": "number"
              }
            }
          }
        }
      '::JSONB
    ),

    (
      'Point',
      '
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "$id": "https://geojson.org/schema/Point.json",
          "title": "GeoJSON Point",
          "type": "object",
          "required": [
            "type",
            "coordinates"
          ],
          "properties": {
            "type": {
              "type": "string",
              "enum": [
                "Point"
              ]
            },
            "coordinates": {
              "type": "array",
              "minItems": 2,
              "items": {
                "type": "number"
              }
            },
            "bbox": {
              "type": "array",
              "minItems": 4,
              "items": {
                "type": "number"
              }
            }
          }
        }
      '::JSONB
    )
  ON CONFLICT DO NOTHING
;
