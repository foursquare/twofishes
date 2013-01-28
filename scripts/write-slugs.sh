echo "
import com.mongodb.casbah.Imports._
GeonamesParser.readSlugs()
GeonamesParser.idToSlugMap.keys.foreach(GeonamesParser.missingSlugList.add)
GeonamesParser.writeMissingSlugs(store)
" | ./sbt indexer/console > output.txt
