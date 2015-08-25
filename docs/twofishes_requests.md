# Twofishes Requests

The fastest way to interact with twofishes is to do it via thrift, which is defined and documented at <https://github.com/foursquare/twofishes/blob/master/interface/src/main/thrift/geocoder.thrift>

The json interfaces are slower, maybe by 20%? (it turns out that string serialization is slow). They mostly mirror the thrift interface.

There are two json interfaces:

1. The combined REST-y interface at /?[parameters]
2. The GET or POST thrift-json interface at /search/[method], one for each method defined in the [thrift service](https://github.com/foursquare/twofishes/blob/master/interface/src/main/thrift/geocoder.thrift#L350)
    1. /search/geocode
    2. /search/reverseGeocode
    3. /search/bulkReverseGeocode
    4. /search/bulkSlugLookup

And there are two debug interfaces

1. /search/geocoder.html - implements geocoding, reverse geocoding and id/slug lookup
2. /search/autocomplete.html -- implements autocomplete
(I keep meaning to add a clickable reverse geocode debugger, but have yet to do so.)

And, there is the thrift interface. Almost all of the descriptions below apply to the thrift requests as well.

When booting up the Geocoder server, note that the JSON API is accessed
from `$HOSTNAME:$API_PORT` where `$API_PORT` tends to be `1 + $PORT` from
running: `./serve.py $HFILE_DIR -p $PORT`. For example, if your command
was `./serve.py $HFILE_DIR -p 6000`, it'll probably be on port 6001.
Verify this by looking for where http/json gets initialized in the log
file. For example, in the case of $PORT = 6000 above:

`[info] 18:20:40.103 [main] INFO  c.f.twofishes.GeocodeFinagleServer$ - serving http/json on port 6001`

## The Combined & Debug Interfaces
Almost every parameter in [GeocodeRequest](https://github.com/foursquare/twofishes/blob/master/interface/src/main/thrift/geocoder.thrift#L243) is implemented as a GET parameter.

The combined json interface allows you to construct queries out of these parameters like <http://demo.twofishes.net/?query=new%20york&lang=es&maxInterpretations=4>

These queries also work as parameters to the debug interface either after the ? or the # as seen at <http://demo.twofishes.net/static/geocoder.html#query=new%20york&lang=es&maxInterpretations=4> or <http://demo.twofishes.net/static/geocoder.html?query=new%20york&lang=es&maxInterpretations=4>

- query=[geocode query] -- For geocoding. The server will attempt to geocode as much of the string (from left to right) as possible
- ll=[lat,lng] -- latlng as comma separated double ie, 40.74,-74.0
    - if query is specified, this will be a hint to bias returned results
    - if query is not specified, it will reverse geocode this latlng
- lang=[two letter language code] -- i.e. es, en or zh. Influences what languages are used in constructing the displayName in the response. Defaults to en. geocoding "nyc" in with lang=es will suggest "Nueva York" as the best name. Additionally, the server will return all alternate names in english + this language.
- cc=[two letter country code] -- biases geocoding similar to ll (prefer matches in this country). Also influences name formatting -- a feature in this country will not include the country name in the displayName
- debug=[integer] -- specify the amount of debugging in the server response. 1 is a lot of debug info, 4 is a tremendous amount of debug info. Slows down the server a lot.
- maxInterpretations=[integer] -- specify the number of different interpretations of the query (or reverse geocode) that you want. Larger value means slower & bigger response.
    - in reverse geocode mode, 0 or unset means 'unlimited'
    - in autocomplete mode, 0 or unset means 3
    - in geocode mode, 0 or unset means 1
- woeHint=[comma separated list of YahooWoeType integers or enum names] - ie woeHint=7,10 or woeHint=TOWN,ADMIN3 - look at the [YahooWoeType enum](https://github.com/foursquare/twofishes/blob/master/interface/src/main/thrift/geocoder.thrift#L3) for total range of values. Biases the geocoder to prefer features of these types
- woeRestrict=[comma separated list of YahooWoeType integers or enum names] - same arguments as woeHint. Geocoder will *only* return features of these types.
- responseIncludes=[comma separated list of ResponseIncludes integers or enum names] - ie responseIncludes=PARENTS,ALL_NAMES - influences what is contained within the returned interpretations. Most of these make the response slightly slower.
    - EVERYTHING - includes everything listed below
    - PARENTS - include the parent features of a venue (ie NY state, US inside the interpretation for 'new york city')
    - ALL_NAMES - include all names, not just in english + lang
    - PARENT_ALL_NAMES - same, but for parents
    - WKB_GEOMETRY - include polygon boundary geometry in WKB format if available
    - WKT_GEOMETRY - include polygon boundary geometry in WKT format if available
    - WKB_GEOMETRY_SIMPLIFIED - include simplified polygon boundary geometry in WKB format if available
    - WKT_GEOMETRY_SIMPLIFIED - include simplified polygon boundary geometry in WKT format if available
    - S2_COVERING - include S2 covering of polygon boundary as a list of S2 cell ids if available
    - S2_INTERIOR - include S2 interior covering of polygon boundary as a list of S2 cell ids if available
    - REVGEO_COVERAGE - for ll+radius revgeo queries, turns on computing the percentage overlap between the circle query and the feature polygon. slow-ish
    - DISPLAY_NAME -  controls if we should fetch parents to construct a string like "New York, New York, US" for legacy reasons, this is automatically turned on for geocode queries for now. It's mainly here because reverse geocode clients often don't need it
- radius=[radius in meters as an integer] -- defaults to 0 (point reverse geocode). If specified along with ll, will return all polygons touched by the circle defined by ll+radius. In strict geocoding mode, used for determining containment.

### Geocoder Only Paramters
- autocomplete=[true/false] -- Defaults to false, whether or not to return partial-  matches as if powering an autocompleter
- autocompleteBias=[AutocompleteBias integer or enum name] - i.e. autocompleteBias=BALANCED - influences how locally (relative to ll hint, if specified) and globally relevant results are mixed in autocomplete geocoding mode. Defaults to BALANCED.
    - NONE - no bias
    - BALANCED - mix locally and globally relevant results (DEFAULT)
    - LOCAL - prefer locally relevant results
    - GLOBAL - prefer globally relevant results
- strict=[true/false] -- Defaults to false, if true, only returns results within the cc, bounds and ll+radius specified

### Bulk Revgeo Requests
- method=bulkrevgeo - in this mode, ll is allowed to occur multiple times, each one is reverse geocoded. This method gains some efficiency in the response size if the points are closely clustered, but the server work happens in serial, so it is slower than executing a number of requests in parallel client-side. Working on it. ex <http://demo.twofishes.net/?ll=41.793252,12.48729&ll=40.74,-74&method=bulkrevgeo>
- method=bulksluglookup - in this mode, slug is allowed to occur multiple times.


### Not Super Useful Parameters for most people
- slug=[string] - if you turn on 'slug generation' in index building, the index will contain a somewhat human readable url-friendly string for every place in the index. This allows you to look up that name and get back the feature. Additionally, it can also take the 'longId' and the 'id' such as '72057594043056517' 'geonameid:5128581'
- allowedSources=[comma separated list of strings] - only useful if you have modified the indexer to have a source other than geonames.


## The Thrift-JSON interface
The endpoints rooted at /search take via GET or POST a json representation of the thrift request that they expect.

To construct one of these, look at the thrift definition and make a json dict that looks roughly the same.

ex.

[http://demo.twofishes.net/search/reverseGeocode?json={ "ll" : { "lat" : 41.793252, "lng" : 12.48729 }, "woeRestrict" : [ 7, 12 ] }](http://demo.twofishes.net/search/reverseGeocode?json={%20%22ll%22%20:%20{%20%22lat%22%20:%2041.793252,%20%22lng%22%20:%2012.48729%20},%20%22woeRestrict%22%20:%20[%207,%2012%20]%20})
[http://demo.twofishes.net/search/geocode?json={"query" : "London, UK"}](http://demo.twofishes.net/search/geocode?json={%22query%22%20:%20%22London,%20UK%22})

## The Thrift Interface
The descriptions of the json interface also describe each of the parameters in the thrift structures. They share the same names and types.

The only differences are that for BulkSlugLookup and BulkReverseGeocode, you have to use the BulkSlugLookupRequest and BulkReverseGeocodeRequest structs, where most of the parameters above are pushed into the 'CommonGeocodeRequestParams params' field.

For single reverse geocodes, you must use the reverseGeocode endpoint, even though it shares its request definition with the geocode endpoint.

You can talk to the thrift interface via finagle-thrift or vanilla (apache) thrift. See [Finagle Docs](https://github.com/twitter/finagle#Simple%20Client%20and%20Server%20for%20Thrift) for accessing the finagle-thrift implementation via scala.

See the [Apache Thrift Docs for Java](http://thrift.apache.org/tutorial/java/), or [tkang's python-thrift blogpost](http://tkang.blogspot.com/2010/07/thrift-server-client-in-python.html) or google around for how to write a thrift client in your langauge of choice.

## Next Time
Documenting the twofishes response!
