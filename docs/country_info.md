# CountryInfo

Based on http://download.geonames.org/export/dump/countryInfo.txt and http://download.geonames.org/export/dump/timeZones.txt


## Overview

A scala library for retrieving basic country metadata.

### Usage

There are some functions in com.foursquare.geo.country.CountryUtils to perform the most common tasks

* CountryUtils.getNameByCode(iso2_or_3) - takes an iso2 or iso3 code and returns a reasonable english name for the country

* CountryUtils.bestLang(iso2_or_3) - returns the most common language in the country

* CountryUtils.getLangs(iso2_or_3) - returns all official languages in that country (with -XX variants like cn-HK stripped down to cn)

* CountryUtils.getLangsWithVariants(iso2_or_3) - same as getLangs but does not strip variants.

You can also pull the full set of metadata directly with

    CountryInfo.getCountryInfo(code)

which returns a case class that has many fields as documented in http://download.geonames.org/export/dump/countryInfo.txt

	case class CountryInfo(
	  iso2: String,
	  iso3: String,
	  fips: String,
	  englishName: String,
	  languages: List[String],
	  geonameid: Option[Int],
	  tld: String,
	  population: Int,
	  continent: String,
	  currencyCode: String,
	  currencyName: String,
	  neighbors: List[String],
	  postalCodeRegexString: String,
	  phonePrefix: String,
	  capital: String,
	  areaSqKm: Option[Int]
	)

CountryInfo.tzIDs returns a String list of all Olson timezones in that country as well

### Usage Example

	scala> com.foursquare.geo.country.CountryUtils.getNameByCode("US")
	res0: Option[String] = Some(United States)

	scala> com.foursquare.geo.country.CountryUtils.getNameByCode("KR")
	res1: Option[String] = Some(South Korea)

	scala> com.foursquare.geo.country.CountryUtils.getLangs("DE")
	res2: List[String] = List(de)

	scala> com.foursquare.geo.country.CountryInfo.getCountryInfo("DE")
	res3: Option[com.foursquare.geo.country.CountryInfo] = Some(CountryInfo(DE,DEU,GM,Germany,List(de),2921044,.de,81802257,EU,EUR,Euro,List(CH, PL, NL, DK, BE, CZ, LU, FR, AT),^(\d{5})$,49,Berlin,357021))

	scala> com.foursquare.geo.country.CountryInfo.getCountryInfo("DE").map(_.currencyName)
	res4: Option[String] = Some(Euro)

	scala> com.foursquare.geo.country.CountryInfo.getCountryInfo("DE").map(_.population)
	res5: Option[Int] = Some(81802257)

	scala> com.foursquare.geo.country.CountryInfo.getCountryInfo("DE").map(_.tzIDs)
	res6: Option[Seq[String]] = Some(List(Europe/Berlin, Europe/Busingen))

