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
from typing import List, Dict, Any
from configparser import ConfigParser

from downloader import get_tsv

UNLOCODE_URL = 'http://www.unece.org/fileadmin/DAM/cefact/locode/loc172csv.zip'
UNLOCODE_PART1_PATH = 'geography/2017-2 UNLOCODE CodeListPart1.csv'
UNLOCODE_PART2_PATH = 'geography/2017-2 UNLOCODE CodeListPart2.csv'
UNLOCODE_PART3_PATH = 'geography/2017-2 UNLOCODE CodeListPart3.csv'

COUNTRIES_PATH = 'geography/countries.csv'
LOCATIONS_PATH = 'geography/known_locations.csv'


class Country(dict):
    def __init__(self, attributes):
        # Attributes is an existing dict with properties.
        # Ensure that the following properties are set:
        self.iso_2 = None
        self.iso_3 = None
        self.name = None
        self.aliases = []
        self.region = None
        self.subregion = None


class countrylist(dict):
    # countrylist is a list of searchkey -> country
    # where each country 
    def __missing__(self, key):
        logging.warning("Unkown country %s" % (key))
        return {'name': key, 'in_eu': False}
    
    @staticmethod
    def from_file(filename=COUNTRIES_PATH):
        """Given a list of countries as a dict,
        with at least 'iso-2', 'iso-3', 'name', 'aliases' attributes, 
        augment with 'in_eu' attribute: True or False
        return a dict with any key, pointing to the augmented dict.
        """
        country_dict = countrylist()
        country_list = get_tsv(filename)
        for country in country_list:
            if country['eu_member_status'] in ('EU member', 'H2020 Associated Country',):
                country['in_eu'] = True
            elif country['eu_member_status'] in ('No', 'Council of Europe', \
                            'EU customs member', 'EU trade ass. Member', 'No'):
                country['in_eu'] = False
            else:
                logging.warning("Unknown EU member status '%s'" % (country['eu_member_status']))
                country['in_eu'] = False
            country_dict[country['iso-2']] = country
            country_dict[country['iso-3']] = country
            country_dict[country['country']] = country
            try:
                country['aliases'] = [alias.strip() for alias in country['aliases'].split(';')]
                for alias in country['aliases']:
                    country_dict[alias] = country
            except KeyError:
                pass
        return country_dict
    
    def to_file(filename):
        pass


class Place:
    def __init__(self):
        self.unlocode = None
        self.country = None
        self.town = None
        self.long = None
        self.lat = None
        self.top500_id = []
        self.meril_id = []
        self.source = 'local'

class placelist(dict):
    # place is a dict: id -> place
    def from_file(filename):
        pass
    
    def to_file(filename):
        pass


class Locator(object):
    def __init__(self, downloader):
        self.location_path = LOCATIONS_PATH
        self.locodes = None
        self.countries = countrylist.from_file(COUNTRIES_PATH)
        self.eu_countries = [k for k,v in self.countries.items() if v['in_eu']]
        self.places = get_tsv(self.location_path)
        self.place_by_id = {}
        self.downloader = downloader
        for place in self.places:
            place['top500_id'] = [int(id) for id in place['top500_id'].split(';') if id]
            for id in place['top500_id']:
                if id in self.place_by_id:
                    logging.error("Duplicate Top 500 ID %d" % (id))
                self.place_by_id[int(id)] = place
            try:
                place['long'] = float(place['long'])
                place['lat'] = float(place['lat'])
            except ValueError:
                pass
        try:
            config = ConfigParser()
            config.read('config.ini')
            self.googlemap_apikey = config['Google']['api_key']
        except Exception as exc:
            logging.error("Can't read Google map api key from config.ini: %s" % (exc))
            self.googlemap_apikey = None
    
    def read_known_countries(self):
        """Populate self.places with known places"""
        # TODO: to be written
    def store_known_countries(self):
        """If self.places was modified, write the modifications to file"""
        # TODO: to be written
    
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
    
    def _get_known_place(self, countrycode, town):
        """Search the local CSV file for the given town.
        Return a dict with attributes: unlocode, countrycode, town, long, lat.
        Return None if the town is not found."""
        for place in self.places:
            if place['countrycode'] == countrycode and place['town'] == town:
                if not place['long'] and not place['lat']:
                    logging.error("No geo location for %s %s" % (countrycode, town))
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
        long = (1 if m.group(3) == 'N' else -1) * (int(m.group(1)) + int(m.group(2)) / 60)
        lat = (1 if m.group(6) == 'E' else -1) * (int(m.group(4)) + int(m.group(5)) / 60)
        return (long, lat)
    
    def _search_locode(self, countrycode, town):
        """Search the UN/LOCODE database for give town.
        Return a dict with attributes: unlocode, countrycode, town, long, lat.
        Return None if the town is not found."""
        if not self.locodes:
            countryfilter = self.filter_factory(self.eu_countries)
            self.locodes = self.get_unlocodes(countryfilter)
        # TODO: use NFKC normalization to make matching even better.
        fuzzymatcher = difflib.SequenceMatcher(False, town.lower(), '')
        for locode in self.locodes:
            if locode['country'] == countrycode:
                fuzzymatcher.set_seq2(locode['ascii'].lower())
                similarity = fuzzymatcher.ratio()
                fuzzymatcher.set_seq2(locode['name'].lower())
                similarity = max(similarity, fuzzymatcher.ratio())
                if similarity > 0.92:
                    long, lat = self._parse_geo84(locode['geo84'])
                    if long is None:
                        logging.error("No geo location for %s %s" % \
                                (locode['country'], locode['place']))
                    place = {'unlocode': locode['country'] + ' ' + locode['place'],
                                'countrycode': locode['country'],
                                'town': locode['name'],
                                'long': long,
                                'lat': lat,
                                'top500_id': [],
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
            'countycodes': countrycode,
            'city': town,
        }
        url = 'https://nominatim.openstreetmap.org/search?' + urlencode(params)
        data = self.downloader.get_uncached_url(url)
        result = json.loads(data)
        result = result["results"][0]
        # TODO: don't cache, but instead add to self.places and write self.places back to file
        print(result)
        result = result[0]
        place = {
                'countycodes': countrycode,
                'city': town,
                'long': float(result["lon"]),
                'lat': float(result["lat"]),
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
                'long': float(result["geometry"]["location"]["long"]),
                'lat': float(result["geometry"]["location"]["lat"]),
                'source': 'googlemaps',
            }
        return place
    
    def _get_place(self, location):
        """Given a dict with 'country' and 'town' attribute, 
        find an associated place dict"""
        # Try to find by identifier in known places
        identifier = location.get('top500_id')
        if identifier in self.place_by_id:
            place = self.place_by_id[identifier]
            if not place['long'] and not place['lat']:
                logging.error("No geo location for %s %s" % \
                             (place['countrycode'], place['town']))
            return place
        # Try to find by country/town in known places
        town = location.get('town', '')  # MAY be defined
        place = self._get_known_place(location['countrycode'], town)
        if place:
            place['source'] = 'local'
            return place
        # Try to find by country/town in known places
        place = self._search_locode(location['countrycode'], town)
        if place:
            place['source'] = 'unlocode'
            return place
        
        return place
        
        # TODO: the following is yet disabled.
        
        # Try to find by address in Open Street Maps
        place = self._get_location_from_osm(location['countrycode'], town)
        if place:
            place['source'] = 'osm'
            return place
        # Try to find by address in Google Maps
        place = self._get_location_from_googlemaps(location['countrycode'], town)
        if place:
            place['source'] = 'googlemaps'
        return place
    
    def _augment(self, location, place):
        for name in ('unlocode', 'country', 'countrycode', 'town', 'long', 'lat'):
            if place.get(name) and not location.get(name):
                location[name] = place[name]
    
    def locate(self, location):
        """Given a dict with 'country' (required) and 'town' and/or 'address' attribute.
        try to augment it countrycode, longitude (long) and latitude (lat) attributes.
        First try UN/LOCODE databae (and set unlocode attribute), 
        otherwise use the Google Maps API."""
        # Set country and countrycode
        try:
            if 'country' not in location:
                assert 'countrycode' in location
                location['country'] = self.countries[location['countrycode']]['country']
        except AssertionError:
            logging.error("Location without country nor countrycode: %s" % (location))
        except IndexError:
            logging.error("Unknown country %s in location %s" % \
                         (location['countrycode'], location))
        try:
            if 'countrycode' not in location:
                country = self.countries[location['country']]
                location['countrycode'] = country['iso-2']
        except IndexError:
            logging.error("Unknown country %s in location %s" % \
                          (location['country'], location))
        # Find geo location
        place = self._get_place(location)
        if not place:
            logging.error("Can't find location for %s in %s (id %s)" % \
                        (location.get('town'), location['country'], 
                         location['top500_id'] if 'top500_id' in location else ''))
            logging.debug("Location = %r" % (location))
            return
        self._augment(location, place)
        
        return
        # TODO: rewrite:
        # - don't assume id is a top500_id (make it work with meril_id too)
        # - what's with the _str attibutes?
        # - make it more generic
        # - write results to file, instead of asking the user to do that.
        # Improve known locations
        new_id = ('top500_id' in location and location['top500_id'] not in place['top500_id'])
        if new_id:
            place['top500_id'].append(location['top500_id'])
        place['id_str'] = ';'.join(str(id) for id in place['top500_id'])
        place['long_str'] = ('%.4f' % (place['long'])) if place['long'] else ''
        place['lat_str'] = ('%.4f' % (place['lat'])) if place['lat'] else ''
        if place.get('source') == 'unlocode':
            logging.warning("Please add known UN/LOCODE to %s:" % (self.location_path))
            # logging.warning("UNLOCODE    Country    Town    Long    Lat    id    Source")
            logging.warning("Please Add:	{unlocode}	{countrycode}	{town}	" \
                        "{long_str}	{lat_str}	{id_str}	".format_map(place))
        elif new_id:
            place['top500_id'].append(location['top500_id'])
            place['id_str'] = ';'.join(str(id) for id in place['top500_id'])
            logging.warning("Please add identifier to %s in %s:" % \
                        (location['unlocode'], self.location_path))
            # logging.warning("UNLOCODE    Country    Town    Long    Lat    id    Source")
            logging.warning("Please Update:	{unlocode}	{countrycode}	{town}	" \
                        "{long_str}	{lat_str}	{id_str}	".format_map(place))

    def get_unlocodes(self, countryfilter=None) -> List[Dict]:
        """Return a list of UN/LOCODE objects (list of dicts).
        Filter by country. countries is a list of 2-character country codes."""
        def get_csv(path) -> List[Dict[str, Any]]:
            return get_tsv(path, encoding='latin-1', 
                    dialect='excel', header=['change', 'country', 'place', 'name', 'ascii', 
                    'province', 'function', 'status', 'date', 'iata', 'geo84', 'note'],
                    filter_func=countryfilter)
        locodes = []  # type: List[Dict[str, Any]]
        locodes = list(chain(
                    get_csv(UNLOCODE_PART1_PATH),
                    get_csv(UNLOCODE_PART2_PATH),
                    get_csv(UNLOCODE_PART3_PATH)))
        return locodes

    def locate_and_filter_places(self, sites, filter_countries=None):
        # TODO: filter countries if set.
        unlocated_keys = []
        for key, site in sites.items():
            assert isinstance(site, dict)
            if 'long' not in site or 'lat' not in site or 'unlocode' not in site:
                self.locate(site)
                if not site.get('long'):
                    unlocated_keys.append(key)
            if key not in unlocated_keys:
                if isinstance(site['long'], str):
                    site['long'] = float(site['long'])
                if isinstance(site['lat'], str):
                    site['lat'] = float(site['lat'])
        for key in unlocated_keys:
            del sites[key]
