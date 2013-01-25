echo "
import com.mongodb.casbah.Imports._
GeonamesParser.readSlugs()
val outputter = new OutputHFile(\".\", false, GeonamesParser.slugEntryMap)
outputter.process()
" | ./sbt indexer/console > output.txt
