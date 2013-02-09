echo "
val outputter = new OutputHFile(\"latest\", false, new SlugEntryMap())
outputter.buildRevGeoIndex()
" | ./sbt indexer/console
