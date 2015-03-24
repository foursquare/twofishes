If you put a geojson (or shapefile) in data/private/polygons, along with a .mapping.json, the indexer will attempt to match those polygons to geonames feature. If you specify  --create_unmatched_features true to the parser, it will generate new features in the index for features it cannot match to geonames, and will attempt to fill in the geographic hierarchy based on the other polygons in the index.

- Generate some GeoJSON 
- Put your .geojson file into `data/private/polygons/` (e.g. supervenue_shapes.geojson)
- Create a .mapping.json file with the same name (e.g. supervenue_shapes.mapping.json) and fill it with something like:

  ```json
      {
        "source": "fsq",
        "nameFields": {  
          "unk": ["name"]
        },
        "woeTypes": [["POI"], ["AIRPORT", "PARK", "SPORT", "UNKNOWN"]],
        "idField": "id"
      }
  ```

  - “nameFields” refers to the field in your geojson “properties” field that contains the POI’s name.
  - “idField” refers to the field in your geojson “properties” field that contains the POI’s ID.  Twofishes needs a 32-bit integer for its ID.
  - “woeTypes” specifies the woe types that will be used to try to match this feature to a geonames feature. Matches in the first list of woe types will be preferred over matches in the second, second over the third, etc. If the first list is of size 1, then that woe type will be used when creating a new feature in the data.
  - “source” is a namespace that will be used below.
- Modify StoredFeatureId.scala to add a new namespace and ID type with the same name as the “source” field above.  
- Build an index! with `--create_unmatched_features true`
  
  ```sh
    ./parse.py output/ --  --create_unmatched_features true
  ```  
    
- Serve it!

  ```sh
    ./serve.py output/
  ```
- View it!  http://localhost:8081/static/geocoder.html 


