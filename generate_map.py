#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate GEOJSON data file, based on the following sources:
* all datacenters in Europe in the top500.org list
* GEANT and GLIF network
* Use UNLOCODE to map cities to geo-coordinates
"""

import sys
from os.path import join, dirname, exists, abspath, getmtime
import logging
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse # Python 2 compatibility
from urllib.request import urlopen
from itertools import chain
from collections import defaultdict
import shutil
import datetime, time
import re
import math
import csv
import json
import difflib
import xml.etree.ElementTree as ET
try:
    import geojson
except ImportError:
    print("Geojson library not found. Install with `pip install geojson`.")
    raise


OUTPUT_FILENAME_NETWORK = 'results/data_cartography_network.geojson'
OUTPUT_FILENAME_SUPERCOMPUTERS = 'results/data_cartography_supercomputers.geojson'
OUTPUT_FILENAME_INSTRUMENTS = 'results/data_cartography_instruments.geojson'

GEANT_URL = 'http://map.geant.org/maps/nodes_and_edges'
GEANT_PATH = 'networks/nodes_and_edges'

TOP500_URL = 'https://www.top500.org/lists/2017/11/download/TOP500_201711_all.xml'
TOP500_PATH = 'sources/TOP500_201711_all.xml'

ESFRI_PATH = 'sources/esfri.csv'

UNLOCODE_URL = 'http://www.unece.org/fileadmin/DAM/cefact/locode/loc172csv.zip'
UNLOCODE_PART1_PATH = 'geography/2017-2 UNLOCODE CodeListPart1.csv'
UNLOCODE_PART2_PATH = 'geography/2017-2 UNLOCODE CodeListPart2.csv'
UNLOCODE_PART3_PATH = 'geography/2017-2 UNLOCODE CodeListPart3.csv'

COUNTRIES_PATH = 'geography/countries.csv'
LOCATIONS_PATH = 'geography/datacenter_locations.csv'


def get_cached_url(url, cache_name=None, ttl=10):
    """Return a Python object from URL or cache file.
    The ttl is time-to-live of the cache file in days."""
    cache_folder = abspath(dirname(__file__))
    pu = urlparse(url)
    if not cache_name:
        # get filename from path and query parameters name *id or *ids.
        # does not include the hostname
        cache_name = pu.path.strip('/')
        cache_name = re.sub(r'[^A-Za-z0-9\._\-]+','_', cache_name)
        for query in pu.query.split('&'):
            try:
                k, v = query.split('=')
                if k[-2:] == 'id' or k[-3:] == 'ids':
                    # replace sequence of non-word characters with _
                    v = re.sub(r'[^A-Za-z0-9]+','_', v)
                    cache_name += '_' + k + '_' + v
            except (ValueError, IndexError):
                pass # ignore any errors
    file_path = join(cache_folder, cache_name)
    if exists(file_path) and time.time() - getmtime(file_path) < ttl*86400:
        # file exists and is recent (<10 days)
        logging.info("Fetching %s" % (cache_name))
        with open(file_path, 'r', encoding='utf-8') as f:
            data = f.read()
    else:
        logging.info("Fetching %s" % (url))
        try:
            response =  urlopen(url)
        except Exception:
            print(url)
            raise
        if response.getcode() != 200:
            logging.warning("Fetching %s returns error code %s" % (url, response.getcode()))
            raise IOError("Failed to download data from %s" % url)
        encoding = 'utf-8'
        # for whatever reason \b is not supported by my implementation of re.
        m = re.search('[\s;]charset=([\w\-]+)', response.getheader('Content-Type'))
        if m:
            encoding = m.group(1)
        data = response.read().decode(encoding)
        try:
            logging.info("Write to %s" % (file_path))
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(data)
        except IOError as e:
            logging.warning("%s" % (e))
            # report and proceed (ignore missing cache)
    return data


def get_tsv(path, encoding='utf-8', dialect='excel-tab', header=None, filter_func=lambda row: True):
    """Return a Python object from tab delimited file.
    The first line is assume to contain headers.
    Returns an list of dicts.
    """
    logging.info("Fetching %s" % (path))
    base_folder = abspath(dirname(__file__))
    path = join(base_folder, path)
    # newline='' allows for multiline fields within quotes
    with open(path, 'r', encoding=encoding, newline='') as csvfile:
        # ignore lines starting with a hash (#) or empty lines
        # Since Python 3.6, filter returns an iterator, so doesn't keep 
        # the whole file in memory.
        filtered_file = filter(lambda row: row.strip() and row[0] != '#', csvfile)
        reader = csv.reader(filtered_file, dialect=dialect)
        # First line MUST be the header.
        if not header:
            header = reader.__next__()
        # remove non-printable characters from headers and turn into lowercase
        header = [re.sub(r'[^A-Za-z0-9\-_]+','_', head.lower()) for head in header]
        return list(filter(filter_func, iter(dict(zip(header, row)) for row in reader)))


def list_to_dict(items, keyname):
    """Turn a list of dicts into a dict of dicts, using the given keyname"""
    return dict((item[keyname], item) for item in items)


def get_unlocodes(countries):
    """Return a list of UN/LOCODE objects (list of dicts).
    Filter by country. countries is a list of 2-character country codes."""
    def get_csv(path):
        return get_tsv(path, encoding='latin-1', 
                dialect='excel', header=['change', 'country', 'place', 'name', 'ascii', 
                'province', 'function', 'status', 'date', 'iata', 'geo84', 'note'],
                filter_func=lambda row: row['country'] in countries)
    locodes = {}
    locodes = list(chain(
                get_csv(UNLOCODE_PART1_PATH),
                get_csv(UNLOCODE_PART2_PATH),
                get_csv(UNLOCODE_PART3_PATH)))
    return locodes


def get_geant_nodes():
    return json.loads(get_cached_url(GEANT_URL, GEANT_PATH))


def get_top500_nodes():
    return ET.XML(get_cached_url(TOP500_URL, TOP500_PATH))


def get_esfri_nodes():
    return get_tsv(ESFRI_PATH)


def export_geojson(path, geoinfo):
    logging.info("Writing %s" % (path))
    for error in geoinfo.errors():
        logging.warning(str(error))
    base_folder = abspath(dirname(__file__))
    path = join(base_folder, path)
    with open(path, "w", encoding='utf-8') as f:
        f.write(geojson.dumps(geoinfo))


def umap_network_layer(geant_nodes, geant_links):
    """Return a geojson.FeatureCollection"""
    # TODO: collapse overlapping line between two same endpoints
    geolayer = geojson.FeatureCollection([])
    for city in geant_nodes:
        geom = geojson.Point((city['long'], city['lat']))
        props = {
                    "name": city['name'],
                    # for geojson.io:
                    # "marker-color": "#7b7e42",
                    # "marker-size": "small",
                    # "marker-symbol": "telephone",
                    # for umap.openstreetmap.fr:
                    "_storage_options": {
                      "iconUrl": "https://raw.githubusercontent.com/sara-nl/data-cartography/master/symbols/exchange.png",
                      "iconClass": "Drop",
                      "color": "#22CCDD",
                      "popupTemplate": "Default",
                      "showLabel": "true",
                      "labelHover": "true",
                      "labelDirection": "bottom"
                    },
                    "capacity": city['capacity'],
                    "link_count": len(city['links'])
                }
        feature = geojson.Feature(geometry=geom, properties=props)
        geolayer['features'].append(feature)
    for link in geant_links:
        geom = geojson.LineString([
                    (link['endpoint1_long'], link['endpoint1_lat']), 
                    (link['endpoint2_long'], link['endpoint2_lat'])])
        props = {
                    "name": link['information'],
                    "capacity": link['capacity'],
                    # for geojson.io:
                    # "marker-color": "#7b7e42",
                    # "marker-size": "small",
                    # "marker-symbol": "telephone",
                    # for umap.openstreetmap.fr:
                    "_storage_options": {
                      "weight": int(math.log(link['capacity'])/2),
                      "opacity": 0.7 if link['region'] == 'europe' else 0.2,
                      "color": "#22CCDD",
                      "popupTemplate": "Default",
                      "showLabel": "true",
                      "labelHover": "true",
                      "labelDirection": "top"
                    }
                }
        feature = geojson.Feature(geometry=geom, properties=props)
        geolayer['features'].append(feature)
    return geolayer


def umap_sc_layer(sc_nodes):
    """Return a geojson.FeatureCollection"""
    # TODO: collapse overlapping line between two same endpoints
    geolayer = geojson.FeatureCollection([])
    for city in sc_nodes:
        geom = geojson.Point((city['long'], city['lat']))
        props = {
                    "name": city['site_name'],
                    # for geojson.io:
                    # "marker-color": "#7b7e42",
                    # "marker-size": "small",
                    # "marker-symbol": "telephone",
                    # for umap.openstreetmap.fr:
                    "_storage_options": {
                      "iconUrl": "https://raw.githubusercontent.com/sara-nl/data-cartography/master/symbols/supercomputer.png",
                      "iconClass": "Drop",
                      "color": "#33FF44",
                      "popupTemplate": "Default",
                      "showLabel": "true",
                      "labelHover": "true",
                      "labelDirection": "bottom"
                    },
                    "r_max": city['r_max'],
                    "system(s)": city['system_name'],
                    "processors": city['num_processors']
                }
        feature = geojson.Feature(geometry=geom, properties=props)
        geolayer['features'].append(feature)
    return geolayer


def umap_instruments_layer(esfri_data):
    """Return a geojson.FeatureCollection"""
    # TODO: collapse overlapping line between two same endpoints
    geolayer = geojson.FeatureCollection([])
    for city in esfri_data:
        geom = geojson.Point((city['long'], city['lat']))
        props = {
                    "name": city['full_name'],
                    # for geojson.io:
                    # "marker-color": "#7b7e42",
                    # "marker-size": "small",
                    # "marker-symbol": "telephone",
                    # for umap.openstreetmap.fr:
                    "_storage_options": {
                      "iconUrl": "https://raw.githubusercontent.com/sara-nl/data-cartography/master/symbols/microscope.png",
                      "iconClass": "Drop",
                      "color": "#EE3322",
                      "popupTemplate": "Default",
                      "showLabel": "true",
                      "labelHover": "true",
                      "labelDirection": "bottom"
                    },
                }
        feature = geojson.Feature(geometry=geom, properties=props)
        geolayer['features'].append(feature)
    return geolayer


def parse_and_filter_network(geant_data, countries):
    """Filter EU sites and links.
    Augment with total capacity per site.
    Augment the links with geo location.
    Returns a list of sites and a list of links."""
    all_geant_nodes = {}
    for city in geant_data["cities"]:
        city['links'] = []
        city['long'] = float(city['long'])
        city['lat'] = float(city['lat'])
        all_geant_nodes[int(city['id'])] = city
    geant_links = {}
    # geant_data["links"] contains a dict: continent -> list of links
    for link in chain.from_iterable(geant_data["links"].values()):
        city1 = all_geant_nodes[int(link["endpoint1_id"])]
        city2 = all_geant_nodes[int(link["endpoint2_id"])]
        if not countries[city1["country_code"]]['in_eu'] and \
                 not countries[city2["country_code"]]['in_eu']:
            continue
        elif countries[city1["country_code"]]['in_eu'] and \
                countries[city2["country_code"]]['in_eu']:
            link['region'] = 'europe'
        else:
            link['region'] = 'international'
        link["endpoint1_long"] = city1['long']
        link["endpoint1_lat"] = city1['lat']
        link["endpoint2_long"] = city2['long']
        link["endpoint2_lat"] = city2['lat']
        if link['capacity'] == 'Fibre link':
            link['capacity'] = 400000  # 400 Gbps
        else:
            link['capacity'] = int(link['capacity'])
        city1['links'].append(link)
        city2['links'].append(link)
        geant_links[int(link['id'])] = link
    geant_nodes = []
    for city in all_geant_nodes.values():
        if not countries[city["country_code"]]['in_eu']:
            continue
        city['capacity'] = sum(link['capacity'] for link in city['links'])
        geant_nodes.append(city)
    return geant_nodes, geant_links.values()


def parse_and_filter_sc(sc_data, countries):
    """Filter EU sites.
    Augment with total capacity per site.
    Augment the links with geo location.
    Returns a list of sites and a list of links."""
    # TODO: combine same sites
    sites = {}
    # namespace handling by ElementTree is rather crappy. lxml is better, but overkill for here.
    m = re.search(r'({[^}]*})\w+', sc_data.tag)
    if m:
        defaultns = m.group(1)
    else:
        defaultns = ''
    def get_property(elt, name, default=''):
        """Given an XML element, return the text of the XML subnet """
        child_elt = elt.find(defaultns + name)
        if child_elt is None:
            logging.warning("Element not found: %s" % (name))
            return ''
        elif child_elt.text:
            return child_elt.text
        else:
            return default
    # print(len(sc_data))
    # print(type(sc_data.tag), sc_data.tag)
    # print(sc_data.attrib)
    for elt in sc_data:
        site_elt = elt.find(defaultns + 'installation-site')
        site_id = int(get_property(site_elt, 'site-id'))
        country = get_property(elt, 'country')
        if not countries[country]['in_eu']:
            continue
        site = {'country': country,
                'town': get_property(elt, 'town'),
                'year': get_property(elt, 'year'),
                'num_processors': int(get_property(elt, 'number-of-processors')),
                'id': site_id,
                'site_name': get_property(site_elt, 'installation-site-name'),
                'site_address': get_property(site_elt, 'installation-site-address'),
                'r_max': float(get_property(elt, 'r-max')),
                'power': float(get_property(elt, 'power', 0.0)),
                'system_name': get_property(elt, 'system-name')}
        if site_id in sites:
            # TODO: merge sites[site_id] and site
            for name in ('country', 'town', 'site_name', 'site_address'):
                if sites[site_id][name] != site[name]:
                    logging.warning('Top 500 site %d: %s is either %r or %r.' %
                                (site_id, name, sites[site_id][name], site[name]))
            sites[site_id]['num_processors'] += site['num_processors']
            sites[site_id]['r_max'] += site['r_max']
            sites[site_id]['power'] += site['power']
            sites[site_id]['system_name'] += ("; " + site['system_name'])
        else:
            sites[site_id]= site
    return sites


def parse_and_filter_instruments(esfri_data, countries):
    for city in esfri_data:
        city['long'] = float(city['long'])
        city['lat'] = float(city['lat'])


class Locator(object):
    def __init__(self, eu_countries):
        self.location_path = LOCATIONS_PATH
        self.locodes = None
        self.countries = countries
        self.eu_countries = [k for k,v in countries.items() if v['in_eu']]
        self.places = get_tsv(self.location_path)
        self.place_by_id = {}
        for place in self.places:
            place['id'] = [int(id) for id in place['id'].split(';') if id]
            for id in place['id']:
                self.place_by_id[int(id)] = place
            try:
                place['long'] = float(place['long'])
                place['lat'] = float(place['lat'])
            except ValueError:
                pass
    def _get_known_place(self, countrycode, town):
        """Search the local CSV file for the given town.
        Return a dict with attributes: unlocode, countrycode, town, long, lat.
        Return None if the town is not found."""
        for place in self.places:
            if place['countrycode'] == countrycode and place['town'] == town:
                if not place['long'] and not place['lat']:
                    logging.error("No geo location for %s %s" % (countrycode, town))
                return place
        logging.debug("place %s %s not found in list of %d" % (countrycode, town, len(self.places)))
        return None
    def _parse_geo84(self, geocode):
        """Turn archaic geo84 encoding of UN/LOCODE to long, lat tuple.
        e.g. '4043N 01957E' becomes (40.72, 19.95)"""
        m = re.match(r'(\d\d)(\d\d)(\w) (\d\d\d)(\d\d)(\w)', geocode)
        if not m:
            return None, None
        long = (1 if m.group(3) == 'N' else -1) * (int(m.group(1)) + int(m.group(2))/60)
        lat = (1 if m.group(6) == 'E' else -1) * (int(m.group(4)) + int(m.group(5))/60)
        return (long, lat)
    def _search_locode(self, countrycode, town):
        """Search the UN/LOCODE database for give town.
        Return a dict with attributes: unlocode, countrycode, town, long, lat.
        Return None if the town is not found."""
        if not self.locodes:
            self.locodes = get_unlocodes(self.eu_countries)
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
                            'id': []}
                    return place
                elif similarity > 0.75:
                    logging.info("Possible location match: %s %s (%s) for %s in %s" % \
                            (locode['country'], locode['ascii'],  locode['place'], town, countrycode))
        return None
    def _get_place(self, location):
        """Given a dict with 'country' and 'town' attribute, 
        find an associated place dict"""
        # Try to find by identifier in known places
        identifier = location.get('id')
        if identifier in self.place_by_id:
            place = self.place_by_id[identifier]
            if not place['long'] and not place['lat']:
                logging.error("No geo location for %s %s" % (place['countrycode'], place['town']))
            return place
        # Try to find by country/town in known places
        town = location.get('town', '') # MAY be defined
        place = self._get_known_place(location['countrycode'], town)
        if place:
            place['source'] = 'local'
            return place
        # Try to find by country/town in known places
        place = self._search_locode(location['countrycode'], town)
        if place:
            place['source'] = 'unlocode'
        return place
    def _augment(self, location, place):
        for name in ('unlocode', 'country', 'countrycode', 'town', 'long', 'lat'):
            if place.get(name) and not location.get(name):
                location[name] = place[name]
    def locate(self, location):
        """Given a dict with 'country' and 'town' attribute, 
        try to augment it with a UN/LOCODE (unlocode), countrycode,
        and associated longitude (long) and latitude (lat) attributes."""
        # Set country and countrycode
        if 'country' not in location:
            location['country'] = self.countries[location['countrycode']]['country']
        if 'countrycode' not in location:
            location['countrycode'] = self.countries[location['country']]['iso-2']
        # Find geo location
        place = self._get_place(location)
        if not place:
            logging.error("Can't find UN/LOCODE for %s in %s (id %s)" % \
                    (location.get('town'), location['country'], location['id'] if 'id' in location else ''))
            return
        self._augment(location, place)
        
        # Improve known locations
        new_id = ('id' in location and location['id'] not in place['id'])
        place['id'].append(location['id'])
        place['id_str'] = ';'.join(str(id) for id in place['id'])
        place['long_str'] = ('%.4f' % (place['long'])) if place['long'] else ''
        place['lat_str'] = ('%.4f' % (place['lat'])) if place['lat'] else ''
        if place.get('source') == 'unlocode':
            logging.warning("Please add known UN/LOCODE to %s:" % (self.location_path))
            # logging.warning("UNLOCODE    Country    Town    Long    Lat    id    Source")
            logging.warning("Please Add:	{unlocode}	{countrycode}	{town}	" \
                        "{long_str}	{lat_str}	{id_str}	".format_map(place))
        elif new_id:
            place['id'].append(location['id'])
            place['id_str'] = ';'.join(str(id) for id in place['id'])
            logging.warning("Please add identifier to %s in %s:" % \
                        (location['unlocode'], self.location_path))
            # logging.warning("UNLOCODE    Country    Town    Long    Lat    id    Source")
            logging.warning("Please Update:	{unlocode}	{countrycode}	{town}	" \
                        "{long_str}	{lat_str}	{id_str}	".format_map(place))


def locate_sites(sites, countries):
    locator = Locator(countries)
    unlocated_keys = []
    for key, site in sites.items():
        assert isinstance(site, dict)
        if 'long' not in site or 'lat' not in site or 'unlocode' not in site:
            locator.locate(site)
            if not site.get('long'):
                unlocated_keys.append(key)
    for key in unlocated_keys:
        del sites[key]


class countrylist(dict):
    def __missing__(self, key):
        logging.warning("Unkown country %s" % (key))
        return {'name': key, 'in_eu': False}


def classify_countries(country_list):
    """Given a list of countries as a dict,
    with at least 'iso-2', 'iso-3', 'name', 'aliases' attributes, 
    augment with 'in_eu' attribute: True or False
    return a dict with any key, pointing to the augmented dict.
    """
    country_dict = countrylist()
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


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(levelname)-8s %(message)s')
    countries = classify_countries(get_tsv(COUNTRIES_PATH))
    # locodes = get_unlocodes(eu_countries)
    # print(len(locodes))

    geant_data = get_geant_nodes()
    geant_nodes, geant_links = parse_and_filter_network(geant_data, countries)
    geolayer = umap_network_layer(geant_nodes, geant_links)
    export_geojson(OUTPUT_FILENAME_NETWORK, geolayer)
    
    sc_data = get_top500_nodes()
    sc_nodes = parse_and_filter_sc(sc_data, countries)
    locate_sites(sc_nodes, countries)
    geolayer = umap_sc_layer(sc_nodes.values())
    export_geojson(OUTPUT_FILENAME_SUPERCOMPUTERS, geolayer)

    esfri_data = get_esfri_nodes()
    parse_and_filter_instruments(esfri_data, countries)
    geolayer = umap_instruments_layer(esfri_data)
    export_geojson(OUTPUT_FILENAME_INSTRUMENTS, geolayer)

