echo "
val outputter = new OutputHFile(\"latest\", false, new SlugEntryMap())
outputter.buildRevGeoIndex()
outputter.buildPolygonIndex()
" | ./sbt indexer/console
