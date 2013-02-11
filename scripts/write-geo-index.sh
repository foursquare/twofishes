echo "
val outputter = new OutputHFile(\".\", false, new SlugEntryMap())
outputter.buildRevGeoIndex()
" | ./sbt indexer/console
