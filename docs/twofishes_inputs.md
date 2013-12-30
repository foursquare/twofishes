There is a list of all files in data/custom/ with a short description and examples.

- aliases.txt adds each alias to the index (and marks it as an english ALT_NAME)
- boosts.txt add the value to the score of the feature when ranking, above just its population
- deletes.txt for each name matching a given regex (with \b appended) indexes both the original name and the name without the matched text.
  - For example, having Township of in deletes.txt, it indexes both Township of Brick and Brick when processing Township of Brick.
- ignores.txt ignores given geonames ids.
- moves.txt sets new lat and lng for given geonames ids.
- name-deletes.txt removes given names from the index.
- names.txt marks given names as preferred (they are indexed if not present).
- rewrites.txt for each name matching a given regex indexes the original name and names with the matched text replaced by each word in the comma separated list.
  - For example, having Mount\b|Mt,Mountain,Mtn in rewrites.txt, it indexes Mount Laurel, Mt Laurel, Mtn Laurel and Mountain Laurel when processing Mount Laurel.

For more details see GeonamesParser.scala and IndexerTest.scala.

You have to rebuild the entire index after changing these files.
