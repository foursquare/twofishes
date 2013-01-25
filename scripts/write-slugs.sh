echo "
import com.mongodb.casbah.Imports._
GeonamesParser.readSlugs()
GeonamesParser.idToSlugMap.keys.foreach(GeonamesParser.missingSlugList.add)
GeonamesParser.writeMissingSlugs(store)
val outputter = new OutputHFile(".", false, GeonamesParser.slugEntryMap)
outputter.process()
" | ./sbt indexer/console > output.txt
