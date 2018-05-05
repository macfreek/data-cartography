#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate GEOJSON data file, based on the following sources:
* all datacenters in Europe in the top500.org list
* GEANT and GLIF network
* Use UNLOCODE to map cities to geo-coordinates
"""

# requires Python 3.4 or higher

import sys
from os.path import join, dirname, abspath
import logging
from itertools import chain
import re
import math
import json
# from typing import Iterable, List, Set, Dict
from configparser import ConfigParser
from pathlib import Path
try:
    import geojson
except ImportError:
    print("Geojson library not found. Install with `pip install geojson`.")
    raise

from downloader import get_tsv, CachedDownloader
from geolocator import Locator

OUTPUT_FILENAME_NETWORK = 'results/data_cartography_network.geojson'
OUTPUT_FILENAME_SUPERCOMPUTERS = 'results/data_cartography_supercomputers.geojson'
OUTPUT_FILENAME_INSTRUMENTS = 'results/data_cartography_instruments.geojson'
OUTPUT_FILENAME_MERIL = 'results/data_cartography_meril.geojson'

GEANT_URL = 'http://map.geant.org/maps/nodes_and_edges'
GEANT_PATH = 'sources/geant_nodes_and_edges.json'

TOP500_URL = 'https://www.top500.org/lists/2017/11/download/TOP500_201711_all.xml'
TOP500_PATH = 'sources/TOP500_201711_all.xml'

ESFRI_PATH = 'sources/esfri.csv'

MERIL_INFRASTRUCTURES = 'MERIL/infrastructures.json'
MERIL_ORGANISATIONS = 'MERIL/organisations.json'


def get_geant_nodes(downloader):
    return downloader.get_cached_json(GEANT_URL, GEANT_PATH)


def get_top500_nodes(downloader):
    # TTL = None signifies never to download the file automatically,
    # but ask the user to do so (if the file doesn't exist)
    # The reason is that you need to log in to download the file.
    return downloader.get_cached_xml(TOP500_URL, TOP500_PATH, ttl=None)


def get_esfri_nodes():
    esfri_list = get_tsv(ESFRI_PATH)
    esfri_dict = {node['name']: node for node in esfri_list}
    assert len(esfri_dict) == len(esfri_list)
    return esfri_dict


def get_meril_nodes():
    cache_folder = abspath(dirname(__file__))
    file_path = join(cache_folder, MERIL_ORGANISATIONS)
    with open(file_path, 'r', encoding='utf-8') as f:
        json_with_str_keys = json.load(f)
        organisations = {int(identifier): org for identifier, org in json_with_str_keys.items()}
    file_path = join(cache_folder, MERIL_INFRASTRUCTURES)
    with open(file_path, 'r', encoding='utf-8') as f:
        json_with_str_keys = json.load(f)
        infrastructures = {
                int(identifier): infra 
                for identifier, infra in json_with_str_keys.items()
            }
    
    places = {}
    for identifier, infrastructure in infrastructures.items():
        try:
            locationtypes = infrastructure["Structure"]["typeOfRI"]
        except KeyError as exc:
            # logging.warning("No location type for RI %d: %s" % (identifier, exc))
            locationtypes = []
        try:
            location = infrastructure["Identification"]["location"]
            if len(location) != 1:
                logging.warning("Unknown location %s for RI %s" % (location, identifier))
            location = location[0]
        except KeyError:
            continue
            # rest of data will also be poor. Skip.
        
        # Other address records are not as good
        try:
            provider_country = infrastructure["Structure"]["providerAddress"]
            if len(provider_country) != 1:
                logging.warning("Unknown country %s for RI %s" % (provider_country, identifier))
            provider_country = provider_country[0]
        except (KeyError, IndexError):
            provider_country = None
        locationadr, _, location_country = location.rpartition('(')
        location_country = location_country.strip(' ()')
        local, _, region = locationadr.partition(', PO: ')
        address, _, town = local.rpartition(',')
        town = town.strip()
        address = address.strip()
        zipcode, _, region = region.partition(',')
        zipcode = zipcode.strip()
        region = region.strip(', ')
        locationarray = (address, town, zipcode, region, location_country)
        organsationAddresses = []
        for organisation in infrastructure["organisations"]:
            try:
                organsationAddresses.append(organisations[organisation]["Postal Address"])
            except KeyError:
                pass
        # TODO: do something useful here
        print(identifier, locationtypes, locationarray, organsationAddresses)
        # places[...] = ...
    return places


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
                      "iconUrl": "https://raw.githubusercontent.com" \
                                 "/sara-nl/data-cartography/master/symbols/exchange.png",
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
                      "weight": int(math.log(link['capacity']) / 2),
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
                      "iconUrl": "https://raw.githubusercontent.com" \
                                 "/sara-nl/data-cartography/master/symbols/supercomputer.png",
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
    geolayer = geojson.FeatureCollection([])
    for city in esfri_data.values():
        geom = geojson.Point((city['long'], city['lat']))
        props = {
                    "name": city['full_name'],
                    # for geojson.io:
                    # "marker-color": "#7b7e42",
                    # "marker-size": "small",
                    # "marker-symbol": "telephone",
                    # for umap.openstreetmap.fr:
                    "_storage_options": {
                      "iconUrl": "https://raw.githubusercontent.com" \
                                 "/sara-nl/data-cartography/master/symbols/microscope.png",
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


def umap_meril_layer(meril_data):
    """Return a geojson.FeatureCollection"""
    geolayer = geojson.FeatureCollection([])
    for city in meril_data:
        geom = geojson.Point((city['long'], city['lat']))
        props = {
                    "name": city['full_name'],
                    # for geojson.io:
                    # "marker-color": "#7b7e42",
                    # "marker-size": "small",
                    # "marker-symbol": "telephone",
                    # for umap.openstreetmap.fr:
                    "_storage_options": {
                      "iconUrl": "https://raw.githubusercontent.com" \
                                 "/sara-nl/data-cartography/master/symbols/microscope.png",
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


def parse_and_filter_network(geant_data, country_filter):
    """Filter EU sites and links.
    Augment with total capacity per site.
    Augment the links with geo location.
    Returns a list of sites and a list of links."""
    all_geant_nodes = {}
    for city in geant_data["cities"]:
        city['links'] = []
        city['long'] = float(city['long'])
        city['lat'] = float(city['lat'])
        city['countrycode'] = city['country_code']
        del city['country_code']
        all_geant_nodes[int(city['id'])] = city
    geant_links = {}
    # geant_data["links"] contains a dict: continent -> list of links
    for link in chain.from_iterable(geant_data["links"].values()):
        city1 = all_geant_nodes[int(link["endpoint1_id"])]
        city2 = all_geant_nodes[int(link["endpoint2_id"])]
        local_count = country_filter(city1) + country_filter(city2)
        if local_count == 0:
            continue
        elif local_count == 1:
            link['region'] = 'international'
        else:
            link['region'] = 'europe'
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
        if not country_filter(city):
            continue
        city['capacity'] = sum(link['capacity'] for link in city['links'])
        geant_nodes.append(city)
    return geant_nodes, geant_links.values()


def parse_and_filter_sc(sc_data, country_filter):
    """Collapse systems at different sites into a single place.
    Filter EU sites.
    Augment with total capacity per site.
    Augment the links with geo location.
    Returns a list of sites."""
    sites = {}
    # namespace handling by ElementTree is rather crappy. lxml is better, but overkill for here.
    m = re.search(r'({[^}]*})\w+', sc_data.tag)
    if m:
        defaultns = m.group(1)
    else:
        defaultns = ''
    
    def get_property(elt, name, default=''):
        """Given an XML element, return the text of the XML subelement """
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
        site = {'country': country,
                'town': get_property(elt, 'town'),
                'year': get_property(elt, 'year'),
                'num_processors': int(get_property(elt, 'number-of-processors')),
                'top500_id': site_id,
                'site_name': get_property(site_elt, 'installation-site-name'),
                'site_address': get_property(site_elt, 'installation-site-address'),
                'r_max': float(get_property(elt, 'r-max')),
                'power': float(get_property(elt, 'power', 0.0)),
                'system_name': get_property(elt, 'system-name')}
        if not country_filter(site):
            continue
        if site_id in sites:
            # merge site into sites[site_id]
            for name in ('country', 'town', 'site_name', 'site_address'):
                if sites[site_id][name] != site[name]:
                    logging.warning('Top 500 site %d: %s is either %r or %r.' %
                                (site_id, name, sites[site_id][name], site[name]))
            sites[site_id]['num_processors'] += site['num_processors']
            sites[site_id]['r_max'] += site['r_max']
            sites[site_id]['power'] += site['power']
            sites[site_id]['system_name'] += ("; " + site['system_name'])
        else:
            sites[site_id] = site
    return sites


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, 
                        format='%(levelname)-8s %(message)s')
    config = ConfigParser()
    config.read('config.ini')
    cache_folder = Path(config['Downloader']['cache_folder'])
    downloader = CachedDownloader(cache_folder)
    locator = Locator(downloader=downloader)
    country_filter = locator.filter_factory(locator.eu_countries)

    geant_data = get_geant_nodes(downloader)
    geant_nodes, geant_links = parse_and_filter_network(geant_data, country_filter)
    geolayer = umap_network_layer(geant_nodes, geant_links)
    export_geojson(OUTPUT_FILENAME_NETWORK, geolayer)
    
    sc_data = get_top500_nodes(downloader)
    sc_nodes = parse_and_filter_sc(sc_data, country_filter)
    locator.locate_and_filter_places(sc_nodes)
    geolayer = umap_sc_layer(sc_nodes.values())
    export_geojson(OUTPUT_FILENAME_SUPERCOMPUTERS, geolayer)

    esfri_nodes = get_esfri_nodes()
    locator.locate_and_filter_places(esfri_nodes)  # no filtering
    geolayer = umap_instruments_layer(esfri_nodes)
    export_geojson(OUTPUT_FILENAME_INSTRUMENTS, geolayer)

    meril_nodes = get_meril_nodes()
    locator.locate_and_filter_places(meril_nodes)  # no filtering
    geolayer = umap_meril_layer(meril_nodes)
    export_geojson(OUTPUT_FILENAME_MERIL, geolayer)
