echo "
import com.mongodb.casbah.Imports._
GeonamesParser.readSlugs()
val outputter = new OutputHFile(\"latest\", true, GeonamesParser.slugEntryMap)
outputter.process()
outputter.buildRevGeoIndex()
" | ./sbt indexer/console > output.txt
