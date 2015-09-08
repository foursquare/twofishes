var statusArea = $('#geocodes');

var lookupButton = $('#lookupButton');
var lookupInput = $('#lookup');

var revgeoButton = $('#revgeoButton');
var revgeoInput = $('#revgeo');

var bulkLookupButton = $('#bulkLookupButton');
var bulkLookupInput = $('#bulkLookup');

var bulkRevGeoButton = $('#bulkRevGeoButton');
var bulkRevGeoInput = $('#bulkRevGeo');

var llInput = $('#ll');
var queryInput = $('#query');
var searchButton = $('#search');
var searchForm = $('#searchForm');
var debugInfo = $('#debugInfo');

function initPage() {
  var ll = getParameterByName('ll');
  var slug = getParameterByName('slug');

  var params = decodeURIComponent(window.location.hash.substr(1)) ||
    decodeURIComponent(window.location.search.substr(1));

  var query = getParameterByName('query') || params;

  if (!!slug) { lookupInput.val(slug); }
  else {
    if (!!ll) { setLL(ll); }
    if (!!query) { setQuery(query); }
  }

  geocode(params);
}

function setLL(q) {
  llInput.val(q);
}

function setQuery(q) {
  queryInput.val(q);
}

lookupButton.click(function() {
  geocode("slug=" + lookupInput.val());
  return false;
})

revgeoButton.click(function() {
  geocode("ll=" + revgeoInput.val());
  return false;
})

function rewriteInputIntoBulkLookupQuery(input) {
  var slugs = input.split(',');
  var slugParams = slugs.map(function(s) { return ("slug="+s) }).join('&');
  return slugParams + "&method=bulksluglookup";
}

function rewriteInputIntoBulkRevGeoQuery(input) {
  var lls = input.split('|');
  var llParams = lls.map(function(s) { return ("ll="+s) }).join('&');
  return llParams + "&method=bulkrevgeo";
}

bulkLookupButton.click(function() {
  geocode("bulkLookup=" + bulkLookupInput.val());
  return false;
})

bulkRevGeoButton.click(function() {
  geocode("bulkRevGeo=" + bulkRevGeoInput.val());
  return false;
})

function doSearch() {
  var query = queryInput.val();
  var search = '';
  if (query) {
    search += '&query=' + query;
  }
  var ll = llInput.val();
  if (ll) {
    search += '&ll=' + ll;
  }
  geocode(search);
  return false;
}

function searchOnEnter(event) {
  if (event.which == 13) {
    event.preventDefault();
    searchForm.submit();
  }
}

searchButton.click(doSearch);
searchForm.submit(doSearch);

queryInput.keypress(searchOnEnter);
llInput.keypress(searchOnEnter);

function getParameterByName(name) {
  params = location.search || location.hash;
  name = name.replace(/[\[]/, "\\\[").replace(/[\]]/, "\\\]");
  var regex = new RegExp("[\\?&#]" + name + "=([^&#]*)"),
      results = regex.exec(params);
  return results == null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

function geocode(query) {
  var bulkInputs = [];

  var maxInterpretations = getParameterByName('maxInterpretations') || 3;

  if (query.match(/bulkLookup=/)) {
    maxInterpretations = 0;
    // bulk slug lookup
    var params = query.split('&')
    var rewrittenQuery = "";
    for (var i = 0; i < params.length; ++i) {
      var keyval = params[i].split('=');
      if (keyval[0] == 'bulkLookup') {
        rewrittenQuery += rewriteInputIntoBulkLookupQuery(keyval[1]);
        bulkInputs = keyval[1].split(',')
      } else {
        rewrittenQuery += params[i];
      }
    }
    query = rewrittenQuery;
  } else if (query.match(/bulkRevGeo=/)) {
    // bulk revgeo
    maxInterpretations = 0;
    var params = query.split('&')
    var rewrittenQuery = "";
    for (var i = 0; i < params.length; ++i) {
      var keyval = params[i].split('=');
      if (keyval[0] == 'bulkRevGeo') {
        rewrittenQuery += rewriteInputIntoBulkRevGeoQuery(keyval[1]);
        bulkInputs = keyval[1].split('|')
      } else {
        rewrittenQuery += params[i];
      }
    }
    query = rewrittenQuery;
  } else if (query.match(/slug=/) || query.match(/revgeo=/)) {
    // ie, unlimited
    maxInterpretations = 0;
  }

  var url = 'http://' + window.location.host + '/?debug=1'
    + '&responseIncludes=EVERYTHING,WKT_GEOMETRY_SIMPLIFIED,WKB_GEOMETRY_SIMPLIFIED'

  var queryParams = ''

  if (query.match(/.*=.*/)) {
    queryParams += '&' + query
  } else if (query.match(/^([-+]?\d{1,2}([.]\d+)?),\s*([-+]?\d{1,3}([.]\d+)?)$/)) {
    maxInterpretations = 10
    queryParams += '&ll=' + query
  } else {
    queryParams += '&query=' + query
  }

  history.pushState(null, null, '?' + queryParams.replace(/^&+/, ''))

  if (queryParams.indexOf('maxInterpretations') != -1) {
    queryParams += '&maxInterpretations=' + maxInterpretations
  }
  url += queryParams;

  debugInfo.empty();

  $.getJSON(url,
    function(data) { 
      debugInfo.append('raw search: ' + '<a href="' + url + '">' + url + '</a>');
      return success(data, bulkInputs)
    }
  ).error(function(jqXHR, textStatus, errorThrown) {
    debugInfo.append($('<font color="red">ERROR: <br/> ' + textStatus + ' <br/> ' + errorThrown.toString() +
        '<br/>' + 'probably want to debug @ <a href="' + url + '">' + url + '</a>'));
  });
}

function fixFeature(feature) {
  var woeTypeName = _.invert(YahooWoeType)[feature.woeType]
  feature.woeType = feature.woeType + ' (' + woeTypeName + ')'

  _.each(feature.names, function(name) {
    name.flags = _.map(name.flags, function(flag) {
      var flagName = _.invert(FeatureNameFlags)[flag]
      return flag + ' (' + flagName + ')';
    });
  });
}

function success(data, bulkInputs) {
  window.console.log(data);

  statusArea.empty();

  function linkifySlugs(str, group1, group2) {
    var id = group1 + ':' + group2;
    return '<a href="/static/geocoder.html#slug=' + id + '">' + id + '</a>';
  }

  _(data.debugLines).each(function(l) {
    l = l.replace(/(geonameid|maponics):(\d+)/g, linkifySlugs);
    debugInfo.append($('<span>' + l + '</span>'));
    debugInfo.append($('<br>'));
  })

  // For bulk replies, we have this side list where the Nth element of the
  // list has all the interpretation indexes that match up to the Nth input.
  var allInterpretationIndexes = [];
  if (data.interpretationIndexes) {
    allInterpretationIndexes = data.interpretationIndexes;
  }

  _(data.interpretations).each(function(interp, count) {
    if (interp.feature) {
      var feature = interp.feature;

      var sectionDiv = $('<div class="result"/>');
      var jsonDiv = $('<div class="json"/>');
      var mapDiv = $('<div class="map" id="result-' + count + '" />');
      statusArea.append(sectionDiv);

      var inputsForThisInterp = [];
      _(allInterpretationIndexes).each(function(interpretationIndexes, inputIndex) {
        window.console.log(interpretationIndexes);
        window.console.log(inputIndex);
        if (interpretationIndexes.indexOf(count) != -1) {
          inputsForThisInterp.push("(Index " + inputIndex + ", Input '" + bulkInputs[inputIndex] + "')")
        }
      });

      var geonameid = _.findWhere(feature.ids, {'source': 'geonameid'})
      var name = feature.highlightedName || feature.displayName || feature.name;
      if (geonameid) {
        name = '<a href="http://geonames.org/' + geonameid.id + '">' + name + '</a>';
      }
      if (interp.what) {
        name = interp.what + ' near ' + name;
      }
      sectionDiv.append('<div class="highlightedName">Interp ' + (count+1) + ': ' + name + '</div>');
      if (allInterpretationIndexes.length > 0) {
        sectionDiv.append(
          '<div class="highlightedName">For input indexes: ' + inputsForThisInterp.join(',') + '</div>');
      }

      var links = []
      _(feature.attributes.urls).each(function(url) {
        var parser = document.createElement('a');
        parser.href = url;
        var link = '[<a href="' + url + '">' + parser.hostname + '</a>]';
        links.push(link)
      });
      if (links.length > 0) {
        sectionDiv.append('<div class="urls">' + links.join(' ') + '</div>');
      }

      sectionDiv.append(jsonDiv);
      sectionDiv.append(mapDiv);

      var opts = {
         layers: new L.TileLayer.MapQuestOpen.OSM(),
        attributionControl: false
      }

      var map = new L.Map('result-' + count, opts);
      feature.geometry.wkbGeometry = null;
      feature.geometry.wkbGeometrySimplified = null;

      fixFeature(feature);
      _.each(interp.parents, fixFeature);

      jsonDiv.html('<pre>' + JSON.stringify(interp, undefined, 2) + '</pre>');

      var center = feature.geometry.center;
      var point = new L.LatLng(center.lat, center.lng)
      var boundingBox = new L.LatLngBounds(point, point);
      boundingBox.extend(point)

      // add the CloudMade layer to the map set the view to a given center and zoom
      map.setView(point, 13)

      // create a marker in the given location and add it to the map
      var marker = new L.Marker(new L.LatLng(center.lat, center.lng));
      var str = JSON.stringify(data, undefined, 2);
      map.addLayer(marker);
      if (feature.geometry.bounds) {
        var bounds = feature.geometry.bounds;
        var p1 = new L.LatLng(bounds.ne.lat, bounds.ne.lng),
            p2 = new L.LatLng(bounds.ne.lat, bounds.sw.lng),
            p3 = new L.LatLng(bounds.sw.lat, bounds.sw.lng),
            p4 = new L.LatLng(bounds.sw.lat, bounds.ne.lng);
        polygonPoints = [p1, p2, p3, p4];

        var polygon = new L.Polygon(polygonPoints);
        map.addLayer(polygon);
        boundingBox.extend(p1);
        boundingBox.extend(p2);
        boundingBox.extend(p3);
        boundingBox.extend(p4);
        map.fitBounds(boundingBox);
      }

      // create a popup to show latlng on right-click
      var popup = L.popup();

      map.on('contextmenu',
        function(e){
          var ll = e.latlng.lat + ',' + e.latlng.lng;
          popup
            .setLatLng(e.latlng)
            .setContent('<a href="?ll=' + ll + '">' + ll + '</a>')
            .openOn(map);
        });

      var wktGeometry = feature.geometry.wktGeometrySimplified || feature.geometry.wktGeometry;
      if (wktGeometry) {
        var wkt = new Wkt.Wkt();
        wkt.read(wktGeometry);
        map.addLayer(wkt.toObject({color: 'blue'}));
      }

      var s2Covering = feature.geometry.s2Covering;
      if (s2Covering) {
        var req = { "cellIdsAsStrings": s2Covering };
        var s2InfoUrl = 'http://' + window.location.host + '/private/getS2CellInfos?json=' + JSON.stringify(req);
        $.getJSON(s2InfoUrl,
            function(data) {
              return successS2Info(data, map, 'red')
            }
          );
      }

      var s2Interior = feature.geometry.s2Interior;
      if (s2Interior) {
        var req = { "cellIdsAsStrings": s2Interior };
        var s2InfoUrl = 'http://' + window.location.host + '/private/getS2CellInfos?json=' + JSON.stringify(req);
        $.getJSON(s2InfoUrl,
            function(data) {
              return successS2Info(data, map, 'green')
            }
          );
      }


      var myIcon = L.icon({
        iconAnchor: [8, 8],
        iconUrl: '/static/leaflet/images/red_dot.png',
      });

      if (data.requestWktGeometry) {
        var wkt = new Wkt.Wkt();
        wkt.read(data.requestWktGeometry)
        map.addLayer(wkt.toObject({
          color: 'green',
          icon: myIcon}));
      }
    }
  });
}

function failure() {
  window.alert('something failed');
}

function successS2Info(data, map, color) {
  if (data.cellInfos) {
    _(data.cellInfos).each(function(cellInfo) {
      var wkt = new Wkt.Wkt();
      wkt.read(cellInfo.wktGeometry)
      map.addLayer(wkt.toObject({color: color, weight: 1}).bindPopup('id: ' + cellInfo.id + '<br>level: ' + cellInfo.level));
    });
  }
}
