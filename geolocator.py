#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Keep track of known locations and countries.
For unknown locations, try to find the geo-coordinated in various sources:
* UNLOCODE
* Open Stree Maps (OSM)
* Google Maps

For countries, use the following raw source:
* ISO 3166 for 2- and 3-alpha codes
* UNSD for geographic regions
"""

import logging
from urllib.parse import urlencode
from itertools import chain
import re
import json
import difflib
import unicodedata
from typing import List, Dict, Any
from configparser import ConfigParser

from downloader import CachedDownloader, get_tsv, store_tsv

UNLOCODE_URL = 'http://www.unece.org/fileadmin/DAM/cefact/locode/loc172csv.zip'
UNLOCODE_PART1_PATH = 'geography/2017-2 UNLOCODE CodeListPart1.csv'
UNLOCODE_PART2_PATH = 'geography/2017-2 UNLOCODE CodeListPart2.csv'
UNLOCODE_PART3_PATH = 'geography/2017-2 UNLOCODE CodeListPart3.csv'

ISO3166_PATH = 'geography/iso3166.csv'
UNSD_PATH = 'geography/UNSD-Methodology.csv'

COUNTRIES_PATH = 'geography/countries.csv'
LOCATIONS_PATH = 'geography/known_locations.csv'


def read_known_countries() -> Dict[Any, Dict[str, Any]]:
    """Return a list of known countries.
    Given a list of countries as a dict,
    with at least 'iso-2', 'iso-3', 'name', 'aliases' attributes, 
    augment with 'in_eu' attribute: True or False
    return a dict with any key, pointing to the augmented dict.
    """
    country_dict = {}
    
    header_types = {'population': int, 
                    'aliases': lambda ls: ls.split(';'), 
                   }
    country_list = get_tsv(COUNTRIES_PATH, header_types=header_types)
    for country in country_list:
        if country['eu_member_status'] in ('EU member', 'H2020 Associated Country',):
            country['in_eu'] = True
        elif country['eu_member_status'] in ('No',):
            country['in_eu'] = False
        else:
            country['in_eu'] = False
            logging.warning("Unknown EU member status '%s'" % (country['eu_member_status']))
        country['m49'] = None
        country['region'] = ''
        country['source'] = 'local'
        country['aliases'] = [country['name']] + country['aliases']
        country_dict[country['iso-2']] = country
        country_dict[country['iso-3']] = country

    header_types = {'m49_code': int}
    country_list = get_tsv(UNSD_PATH, dialect='excel', header_types=header_types)
    for country_src in country_list:
        name = country_src["country_or_area"]
        country = country_dict.get(country_src['iso-alpha3_code'])
        if not country:
            if not country_src["iso-alpha3_code"]:
                # E.g. Channel island Sark
                logging.debug("Ignore country without ISO-3 code: %s" % (country_src))
                continue
            country = {
                'name': name,
                'in_eu': False,
                'aliases': [name],
                'iso-3': country_src["iso-alpha3_code"],
                'iso-2': None,
                'source': 'unsd',
            }
            country_dict[country['name']] = country
            country_dict[country['iso-3']] = country
        country['region'] = country_src["sub-region_name"]
        country['m49'] = country_src["m49_code"]
        country_dict[country['m49']] = country
        if name not in country['aliases']:
            country['aliases'].append(name)
    
    header_types = {'numeric': int}
    country_list = get_tsv(ISO3166_PATH, header_types=header_types)
    for country_src in country_list:
        country = country_dict.get(country_src['numeric'])
        if not country:
            # E.g. Taiwan is in ISO 3166 but not in UNSD.
            logging.debug("Unknown country in %s: %s" % (ISO3166_PATH, country_src))
            country = {
                'name': country_src['english_short_name'],
                'in_eu': False,
                'aliases': [name],
                'iso-3': country_src["alpha-3_code"],
                'iso-2': country_src["alpha-2_code"],
                'm49': country_src["numeric"],
                'region': '',
                'source': 'iso3166',
            }
            country_dict[country['name']] = country
            country_dict[country['iso-2']] = country
            country_dict[country['iso-3']] = country
            country_dict[country['m49']] = country
        iso2 = country_src['alpha-2_code']
        if not country['iso-2']:
            country['iso-2'] = country_src['alpha-2_code']
            country_dict[country['iso-2']] = country
        assert country['iso-2'] == country_src['alpha-2_code']
        
        name = country_src['english_short_name']
        if name not in country['aliases']:
            country['aliases'].append(name)
    
    all_country_codes = [country['iso-2'] for country in country_dict.values()]
    all_country_codes = list(set(country_dict))  # remove duplicates
    for countrycode in all_country_codes:
        country = country_dict[countrycode]
        for alias in country['aliases']:
            country_dict[alias] = country
        
        assert country_dict[country['name']] == country
        assert country_dict[country['iso-2']] == country
        assert country_dict[country['iso-3']] == country
        if country['m49']:
            assert country_dict[country['m49']] == country
        assert 'name' in country and country['name']
        assert 'in_eu' in country
        assert 'aliases' in country
        assert 'iso-3' in country and country['iso-3']
        assert 'iso-2' in country and country['iso-2']
        assert 'm49' in country
        assert 'region' in country
    
    return country_dict


class placelist(dict):
    # place is a dict: id -> place
    def from_file(filename):
        pass
    
    def to_file(filename):
        pass


class UnkownLocation(Exception):
    pass


class Locator(object):
    def __init__(self, downloader):
        self.location_path = LOCATIONS_PATH
        self.locodes = None
        self.countries = read_known_countries()
        self.eu_countries = [k for k,v in self.countries.items() if v['in_eu']]
        self.places = self.read_known_places()
        self.place_by_top500_id = {}
        self.place_by_meril_id = {}
        self.downloader = downloader
        for place in self.places:
            place['top500_id'] = [int(id) for id in place['top500_id'].split(';') if id]
            for id in place['top500_id']:
                if id in self.place_by_top500_id:
                    logging.error("Duplicate Top 500 ID %d" % (id))
                self.place_by_top500_id[int(id)] = place
        for place in self.places:
            place['meril_id'] = [int(id) for id in place['meril_id'].split(';') if id]
            for id in place['meril_id']:
                if id in self.place_by_meril_id:
                    logging.error("Duplicate MERIL ID %d" % (id))
                self.place_by_meril_id[int(id)] = place
        try:
            config = ConfigParser()
            config.read('config.ini')
            self.googlemap_apikey = config['Google']['api_key']
        except Exception as exc:
            logging.error("Can't read Google map api key from config.ini: %s" % (exc))
            self.googlemap_apikey = None
    
    def read_known_places(self):
        """Populate self.places with known places"""
        self._modified_places = False
        return get_tsv(self.location_path)
    
    def add_place(self, place, location):
        """Augment place with location attributes, and store in self.places."""
        for key in ('top500_id', 'meril_id'):
            identifier = location.get(key)
            if identifier and identifier not in place[key]:
                place[key].append(identifier)
                self._modified_places = True
        if place not in self.places:
            logging.info("Add known location: %s" % (place))
            self.places.append(place)
            self._modified_places = True
        else:
            logging.info("Augment known location: %s" % (place))
    
    def store_known_places(self):
        """If self.places was modified, write the modifications to file"""
        if not self._modified_places:
            logging.debug("Places not modified.")
            # return
        header = ['UNLOCODE', 'Countrycode', 'Town', 'Long', 'Lat', 
                  'top500_id', 'meril_id', 'Source']
        header_types = {
            'top500_id': lambda itemlist: ';'.join([str(item) for item in itemlist]),
            'meril_id': lambda itemlist: ';'.join([str(item) for item in itemlist]),
            'long': lambda item: "%.4f" % (item) if isinstance(item, float) else '',
            'lat': lambda item: "%.4f" % (item) if isinstance(item, float) else '',
        }
        sort_key = lambda row: (row['countrycode'], row['unlocode'], row['town'])
        store_tsv(self.location_path, 
                self.places, 
                header=header, 
                header_types=header_types, 
                sort_key=sort_key
            )
        self._modified_places = False
    
    def filter_factory(self, countries):
        """Return a filter function `country_filter(place)` that checks 
        if the given place is in one of the given countries."""
        def country_filter(place):
            if 'country' in place:
                return place['country'] in countries
            elif 'countrycode' in place:
                return place['countrycode'] in countries
            else:
                # logging.error("Can't determine country of place %s" % (place))
                raise ValueError("Can't determine country of place %s" % (place))
        return country_filter
    
    def _get_known_place_by_top500_id(self, identifier):
        """Search the local CSV file for the given town.
        Return a dict with attributes: unlocode, countrycode, town, long, lat.
        Return None if the town is not found."""
        if identifier and identifier in self.place_by_top500_id:
            place = self.place_by_top500_id[identifier]
            if not place['long'] and not place['lat']:
                if not place['town']:
                    logging.debug("Known location without known geo location: " \
                                "Top 500 id %d in %s" % (identifier, place['countrycode']))
                else:
                    logging.error("No geo location [TOP500] for %s %s" % \
                                 (place['countrycode'], place['town']))
                raise UnkownLocation()
            return place
        return None

    def _get_known_place_by_meril_id(self, identifier):
        """Search the local CSV file for the given town.
        Return a dict with attributes: unlocode, countrycode, town, long, lat.
        Return None if the town is not found."""
        if identifier and identifier in self.place_by_meril_id:
            place = self.place_by_meril_id[identifier]
            if not place['long'] and not place['lat']:
                if not place['town']:
                    logging.debug("Known location without known geo location: " \
                                "MERIL id %d in %s" % (identifier, place['countrycode']))
                else:
                    logging.error("No geo location [MERIL] for %s %s" % \
                                 (place['countrycode'], place['town']))
                raise UnkownLocation()
            return place
        return None
    
    def _get_known_place(self, countrycode, town):
        """Search the local CSV file for the given town.
        Return a dict with attributes: unlocode, countrycode, town, long, lat.
        Return None if the town is not found."""
        for place in self.places:
            if place['countrycode'] == countrycode and place['town'] == town:
                if not place['long'] and not place['lat']:
                    logging.error("No geo location [known] for %s %s" % (countrycode, town))
                return place
        logging.debug("place %s %s not found in list of %d" % \
                     (countrycode, town, len(self.places)))
        return None
    
    def _parse_geo84(self, geocode):
        """Turn archaic geo84 encoding of UN/LOCODE to long, lat tuple.
        e.g. '4043N 01957E' becomes (40.72, 19.95)"""
        m = re.match(r'(\d\d)(\d\d)(\w) (\d\d\d)(\d\d)(\w)', geocode)
        if not m:
            return None, None
        lat = (1 if m.group(3) == 'N' else -1) * (int(m.group(1)) + int(m.group(2)) / 60)
        long = (1 if m.group(6) == 'E' else -1) * (int(m.group(4)) + int(m.group(5)) / 60)
        return (long, lat)
    
    def _search_locode(self, countrycode, town):
        """Search the UN/LOCODE database for give town.
        Return a dict with attributes: unlocode, countrycode, town, long, lat.
        Return None if the town is not found."""
        if not self.locodes:
            countryfilter = self.filter_factory(self.eu_countries)
            self.locodes = self.get_unlocodes(countryfilter)
        ascii_town = unicodedata.normalize('NFKC', town.lower())
        fuzzymatcher = difflib.SequenceMatcher(False, ascii_town, '')
        
        # TODO: change self.locodes into {country -> list of places}
        
        for locode in self.locodes:
            if locode['country'] == countrycode:
                fuzzymatcher.set_seq2(locode['ascii'].lower())
                similarity = fuzzymatcher.ratio()
                fuzzymatcher.set_seq2(locode['name'].lower())
                similarity = max(similarity, fuzzymatcher.ratio())
                if similarity > 0.92:
                    long, lat = self._parse_geo84(locode['geo84'])
                    if long is None:
                        logging.debug("No geo location in UNLOCODE for %s %s" % \
                                (locode['country'], locode['place']))
                        return None
                    place = {
                            'unlocode': locode['country'] + ' ' + locode['place'],
                            'countrycode': locode['country'],
                            'town': locode['name'],
                            'long': long,
                            'lat': lat,
                            'top500_id': [],
                            'meril_id': [],
                            'source': 'unlocode',
                            }
                    return place
                elif similarity > 0.75:
                    logging.info("Possible location match: %s %s (%s) for %s in %s" % \
                            (locode['country'], locode['ascii'], locode['place'], \
                            town, countrycode))
        return None
    
    def _get_location_from_osm(self, countrycode, town):
        params = {
            'format': 'jsonv2',
            'email': 'freek.dijkstra@surfsara.nl',
            'countrycodes': countrycode,
            'city': town,
        }
        url = 'https://nominatim.openstreetmap.org/search?' + urlencode(params)
        data = self.downloader.get_uncached_url(url)
        result = json.loads(data)
        if not result:
            logging.debug("No result from OSM for %s in %s"  % (town, countrycode))
            return None
        logging.debug("OSM result: %s" % (result))
        result = result[0]
        place = {
                'unlocode': '',
                'countrycode': countrycode,
                'town': town,
                'long': float(result["lon"]),
                'lat': float(result["lat"]),
                'top500_id': [],
                'meril_id': [],
                'source': 'osm',
                }
        return place
    
    def _get_location_from_googlemaps(self, address):
        if not self.googlemap_apikey:
            logging.error("No Google Maps API key available")
            return None
        params = {
            'key': self.googlemap_apikey,
            'query': address,
        }
        url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?' + urlencode(params)
        data = self.downloader.get_uncached_url(url)
        result = json.loads(data)
        # TODO: don't cache, but instead add to self.places and write self.places back to file
        print(result)
        result = result["geometry"]["location"]["lat"]
        place = {
                'unlocode': '',
                'countycode': '',  # TODO: this is bad
                'town': '',  # TODO: this is bad
                'long': float(result["geometry"]["location"]["long"]),
                'lat': float(result["geometry"]["location"]["lat"]),
                'top500_id': [],
                'meril_id': [],
                'source': 'googlemaps',
            }
        return place
    
    def _get_place(self, location):
        """Given a dict with 'country' and 'town' attribute, 
        find an associated place dict"""
        top500_id = location.get('top500_id')
        meril_id = location.get('meril_id')
        countrycode = location.get('countrycode')
        town = location.get('town')
        
        # Try to find by identifier in known places
        place = self._get_known_place_by_top500_id(location.get('top500_id'))
        if place:
            if not place['source']:
                place['source'] = 'local'
            return place

        place = self._get_known_place_by_meril_id(location.get('meril_id'))
        if place:
            if not place['source']:
                place['source'] = 'local'
            return place
        
        if not town:
            # There is nothing more to look for.
            logging.warning("No known town: %s" % location)
            raise UnkownLocation()
        
        # Try to find by country/town in known places
        place = self._get_known_place(location['countrycode'], town)
        if place:
            if not place['source']:
                place['source'] = 'local'
            self.add_place(place, location)
            return place
        
        # Try to find by country/town in UNLOCODE
        place = self._search_locode(location['countrycode'], town)
        if place and place['lat'] and place['long']:
            place['source'] = 'unlocode'
            self.add_place(place, location)
            return place
        
        # Try to find by address in Open Street Maps
        place = self._get_location_from_osm(location['countrycode'], town)
        if place:
            place['source'] = 'osm'
            self.add_place(place, location)
            return place

        return place
        
        # TODO: the following is yet disabled.

        # Try to find by address in Google Maps
        place = self._get_location_from_googlemaps(location['countrycode'], town)
        if place:
            place['source'] = 'googlemaps'
        return place
    
    def _augment(self, location, place):
        """Augment location with place attributes"""
        for name in ('unlocode', 'country', 'countrycode', 'town', 'long', 'lat'):
            if place.get(name) and not location.get(name):
                location[name] = place[name]
    
    def locate(self, location):
        """Given a dict with 'country' (required) and 'town' and/or 'address' attribute.
        try to augment it countrycode, longitude (long) and latitude (lat) attributes.
        First try UN/LOCODE databae (and set unlocode attribute), 
        otherwise use the Google Maps API."""
        # Set country and countrycode
        if not location.get('country'):
            if not location.get('countrycode'):
                # TODO: should be logging.warning, but this is yet too common
                logging.debug("Location without country nor countrycode: %s" % (location))
                raise UnkownLocation()
            try:
                country = self.countries[location['countrycode']]
            except KeyError:
                logging.error("Unknown country %s in location %s" % \
                             (location['countrycode'], location))
                raise UnkownLocation()
            location['country'] = country['country']
        if not location.get('countrycode'):
            try:
                country = self.countries[location['country']]
            except KeyError:
                logging.error("Unknown country %s in location %s" % \
                              (location['country'], location))
                raise UnkownLocation()
            location['countrycode'] = country['iso-2']
        # Find geo location
        
        # TODO: insert _place() method here.
        
        place = self._get_place(location)
        if not place:
            logging.error("Can't find location for %s in %s" % \
                        (location.get('town'), location['country']))
            logging.debug("Location = %r" % (location))
            raise UnkownLocation()
        
        if not place.get('long'):
            # TODO: if the place was found based on ID, this is a known issue.
            # don't report is as error, but only as debug information
            logging.error("Found place without geo-coordinates: %s" % (place))
            raise UnkownLocation()
        if isinstance(place['long'], str):
            logging.error("Found place with geo-coordinates as string: %s" % (place))
        
        self._augment(location, place)

    def get_unlocodes(self, countryfilter=None) -> List[Dict]:
        """Return a list of UN/LOCODE objects (list of dicts).
        Filter by country. countries is a list of 2-character country codes."""
        def get_csv(path) -> List[Dict[str, Any]]:
            header = ['change', 'country', 'place', 'name', 'ascii', 
                    'province', 'function', 'status', 'date', 'iata', 'geo84', 'note']
            return get_tsv(path, encoding='latin-1', dialect='excel', header=header,
                           filter_func=countryfilter)
        locodes = []  # type: List[Dict[str, Any]]
        locodes = list(chain(
                    get_csv(UNLOCODE_PART1_PATH),
                    get_csv(UNLOCODE_PART2_PATH),
                    get_csv(UNLOCODE_PART3_PATH)))
        return locodes

    def locate_and_filter_places(self, locations, filter_countries=None):
        # TODO: filter countries if set.
        unlocated_keys = []
        for key, location in locations.items():
            assert isinstance(location, dict)
            if not location.get('long') or not location.get('lat'):
                try:
                    self.locate(location)
                    # assert isinstance(location['long'], float)
                except UnkownLocation as exc:
                    # error was logged in locate() method.
                    unlocated_keys.append(key)
        for key in unlocated_keys:
            del locations[key]


if __name__ == '__main__':
    import sys
    from pathlib import Path
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, 
                        format='%(levelname)-8s %(message)s')
    # esfri_list = get_tsv('sources/esfri.csv')
    # esfri_dict = {node['name']: node for node in esfri_list}
    # assert len(esfri_dict) == len(esfri_list)
    # locator.locate_and_filter_places(esfri_dict)  # no filtering
    downloader = CachedDownloader(Path('./'))
    locator = Locator(downloader=downloader)
    locator.store_known_places()
    
