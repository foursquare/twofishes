var statusArea = $('#geocodes');

var lookupButton = $('#lookupButton');
var lookupInput = $('#lookup');

var revgeoButton = $('#revgeoButton');
var revgeoInput = $('#revgeo');

var bulkLookupButton = $('#bulkLookupButton');
var bulkLookupInput = $('#bulkLookup');

var bulkRevGeoButton = $('#bulkRevGeoButton');
var bulkRevGeoInput = $('#bulkRevGeo');

var queryInput = $('#query');
var searchButton = $('#search');
var searchForm = $('#searchForm');
var debugInfo = $('#debugInfo');

function initPage() {
  var ll = getParameterByName('ll');
  var query = getParameterByName('query');
  var slug = getParameterByName('slug');
  var urlQuery = decodeURIComponent(getQuery());
  if (!!ll) { setQuery(ll); }
  else if (!!query) { setQuery(query); }
  else if (!!slug) { lookupInput.val(slug); }
  else if (!!urlQuery) { setQuery(urlQuery); }

  geocode();
}

function setQuery(q) {
  queryInput.val(q);
}

function getQuery() {
  query = window.location.hash.substr(1);

  if (query == "") {
    query = window.location.search.substr(1);
  }

  return query
}

lookupButton.click(function() {
  window.location.hash = "lookup=" + lookupInput.val();
  geocode();
  return false;
})

revgeoButton.click(function() {
  window.location.hash = "revgeo=" + revgeoInput.val();
  geocode();
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
  window.location.hash = "bulkLookup=" + bulkLookupInput.val();
  geocode();
  return false;
})

bulkRevGeoButton.click(function() {
  window.location.hash = "bulkRevGeo=" + bulkRevGeoInput.val();
  geocode();
  return false;
})

searchButton.click(function() {
  window.location.hash = queryInput.val();
  geocode();
  return false;
})

searchForm.submit(function() {
  window.location.hash = queryInput.val();
  geocode();
  return false;
})

function getParameterByName(name) {
  params = location.search || location.hash;
  name = name.replace(/[\[]/, "\\\[").replace(/[\]]/, "\\\]");
  var regex = new RegExp("[\\?&#]" + name + "=([^&#]*)"),
      results = regex.exec(params);
  return results == null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

function geocode() {
  var query = getQuery();
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
  } else if (query.match(/lookup=/)) {
    maxInterpretations = 0;
    // one slug lookup
    var params = query.split('&')
    var rewrittenQuery = "";
    for (var i = 0; i < params.length; ++i) {
      var keyval = params[i].split('=');
      if (keyval[0] == 'lookup') {
        rewrittenQuery += 'slug=' + keyval[1];
      } else {
        rewrittenQuery += params[i];
      }
    }
    query = rewrittenQuery;
  } else if (query.match(/revgeo=/)) {
    maxInterpretations = 0;
    // one revgeo
    var params = query.split('&')
    var rewrittenQuery = "";
    for (var i = 0; i < params.length; ++i) {
      var keyval = params[i].split('=');
      if (keyval[0] == 'revgeo') {
        rewrittenQuery += 'll=' + keyval[1];
      } else {
        rewrittenQuery += params[i];
      }
    }
    query = rewrittenQuery;
  }
I
  var url = 'http://' + window.location.host + '/?debug=1'
    + '&responseIncludes=EVERYTHING,WKT_GEOMETRY_SIMPLIFIED,WKB_GEOMETRY_SIMPLIFIED'

  if (query.match(/.*=.*/)) {
    url += '&' + query
  } else if (query.match(/^([-+]?\d{1,2}([.]\d+)?),\s*([-+]?\d{1,3}([.]\d+)?)$/)) {
    maxInterpretations = 10
    url += '&ll=' + query
  } else {
    url += '&query=' + query
  }

  url += '&maxInterpretations=' + maxInterpretations


  $.getJSON(url,
    function(data) { return success(data, bulkInputs) }
  ).error(function(jqXHR, textStatus, errorThrown) {
    debugInfo.empty();
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
  debugInfo.empty();


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
      sectionDiv.append(jsonDiv);
      sectionDiv.append(mapDiv);

      var opts = {
         layers: new L.TileLayer.MapQuestOpen.OSM(),
        attributionControl: false
      }

      var map = new L.Map('result-' + count, opts);
      feature.geometry.wkbGeometry = null;

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

      var wktGeometry = feature.geometry.wktGeometrySimplified || feature.geometry.wktGeometry;
      if (wktGeometry) {
        var wkt = new Wkt.Wkt();
        wkt.read(feature.geometry.wktGeometry);
        map.addLayer(wkt.toObject({color: 'blue'}));
      }

      var myIcon = L.icon({
        iconAnchor: [8, 8],
        iconUrl: '/static/leaflet/images/red_dot.png',
      });

      if (data.requestWktGeometry) {
        var wkt = new Wkt.Wkt();
        wkt.read(data.requestWktGeometry)
        map.addLayer(wkt.toObject({
          color: '#ff0000',
          fillColor: '#ff0000',
          icon: myIcon}));
      } else {
        var ll = getParameterByName('ll')
        if (ll) {
          var parts = ll.split(',');
          var marker = new L.Marker(new L.LatLng(parts[0], parts[1]), {icon: myIcon})
          map.addLayer(marker);
        }
      }
    }
  });
}

function failure() {
  window.alert('something failed');
}
