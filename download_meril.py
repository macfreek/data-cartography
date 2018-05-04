#!/usr/bin/env python3

from html.parser import HTMLParser
import re
import json
import sys
import logging
from configparser import ConfigParser
from pathlib import Path

# local library
from downloader import CachedDownloader


BASE_MERIL_URL = "https://portal.meril.eu"
ALL_INFRASTRUCTURES_URL = BASE_MERIL_URL + "/meril/search/quick?keyword=&" \
    "_type=gr.ekt.cerif.entities.infrastructure.Facility&typeOfSearch=ris&page=1&size=5000"
ALL_ORGANISATIONS_URL = BASE_MERIL_URL + "/meril/search/quick?keyword=&" \
    "_type=gr.ekt.cerif.entities.base.OrganisationUnit&typeOfSearch=other&page=1&size=5000"

ALL_INFRASTRUCTURES_PATH = "infrastructures.html"
ALL_ORGANISATIONS_PATH = "organisations.html"

INFRASTRUCTURE_RE_PATH = r".*/meril/view/facilitys/(\d+)"
ORGANISATION_RE_PATH = r".*/meril/view/organisationUnits/(\d+)"
RELATIONS_URL_PATH = BASE_MERIL_URL + "/meril/view/organisationUnits/%d/facilitys?page=1&pageSize=200"


class SearchResultParser(HTMLParser):
    def __init__(self, base_url, url_regexp=None):
        super(SearchResultParser, self).__init__()
        self.is_search_result = False
        self.result = {}
        self.url_re = re.compile(url_regexp)
        self.base_url = base_url

    def handle_starttag(self, tag, attrs):
        if tag == 'div':
            d = dict(attrs)
            if d.get('class') == 'advSearchResultsLabel':
                self.is_search_result = True
        elif tag == 'a' and self.is_search_result:
            d = dict(attrs)
            href = d.get('href')
            if href:
                m = self.url_re.match(href)
                if m:
                    if href.startswith('/'):
                        href = self.base_url + href
                    id_ = int(m.group(1))
                    self.result[id_] = href
            else:
                logging.error("Unexpected search result with link attrs %s" % attrs)
            self.is_search_result = False


class InfrastructureParser(HTMLParser):
    def __init__(self, identifier):
        super(InfrastructureParser, self).__init__()
        self.identifier = identifier
        self.divtrace = []
        self.do_trace = False
        self.result = {'id': int(identifier)}
        self.sectionname = 'Core'
        self.section = None
        self.subsectionname = None

    def handle_starttag(self, tag, attrs):
        if tag == 'div':
            d = dict(attrs)
            id_ = d.get('id', '')
            class_ = d.get('class', '')
            if id_ == 'main-block':
                self.do_trace = True
            elif self.do_trace:
                trace = id_ if id_ else class_
                self.divtrace.append(trace)
        # elif self.do_trace:
        #     if tag == 'a' and attrs and attrs[0][1].startswith('/meril/view/facilitys/'):
        #         print(self.identifier, attrs[0][1])
        #     # print(tag, attrs)

    # TODO: handle: sup (2x)
    # TODO: handle button the same as div
    # TODO: handle URL without "http://"

    def handle_endtag(self, tag):
        if tag == 'div' and self.do_trace:
            try:
                self.divtrace.pop()
            except IndexError:
                # pass
                self.do_trace = False
        
    def handle_data(self, data):
        if self.do_trace:
            # data = data.strip()
            data = re.sub('\s+', ' ', data).strip()
            # if data and self.divtrace == ['main-block', 'viewPageContentId', 'viewPageContentAccordionId', '', 'riMainSegmentContent']:
            if not data:
                return
            # print(self.divtrace, repr(data))
            # return
            # print(self.divtrace)
            if 'createAndLastUpdateInfo' in self.divtrace:
                return
            elif 'pictures2FrameId' in self.divtrace:
                return
            elif 'viewPageRIPhotosId' in self.divtrace:
                return
            elif 'bread' in self.divtrace:
                return
            elif self.divtrace == ['viewPageHeaderId', 'viewPageRINameId']:
                self.result['name'] = data
                return
            elif self.divtrace == ['viewPageHeaderId', 'viewPageRIURLId', 'viewPageRIURLLinkId']:
                self.result['url'] = data
                return
            elif self.divtrace == ['viewPageContentId', 'viewPageContentAccordionId', '', 'riHorizontalHeader', 'riHorizontalHeaderLabel']:
                self.sectionname = data
                self.section = {}
                self.result[data] = self.section
                return
            elif self.divtrace == ['viewPageContentId', 'viewPageContentAccordionId', '', 'riMainSegmentContent']:
                # if self.subsectionname is not None and self.subsectionname not in self.section:
                #     print(self.identifier, "Found subsection without content: ", self.subsectionname)
                #     print(self.result)
                self.subsectionname = data
                assert self.section is not None
                return
            elif self.divtrace[:6] == ['viewPageContentId', 'viewPageContentAccordionId', '', 'riMainSegmentContent', 'customAccordionPanel', 'viewPageContentDataV2']:
                assert self.section is not None
                if self.subsectionname is None:
                    logging.error("Found subsection in %s without section name: %s" % \
                             (self.identifier, data))
                    self.subsectionname = ''
                # assert self.subsectionname is not None
                if self.subsectionname in self.section:
                    self.section[self.subsectionname].append(data)
                else:
                    self.section[self.subsectionname] = [data]
                return
            elif self.divtrace == ['viewPageContentId']:
                if data == 'Information for this RI entry is currently being completed':
                    self.result['incomplete'] = True
                else:
                    print (data)
                return
            elif len(self.divtrace) < 6:
                print("Unknown web page part: ", self.divtrace)
                return
            elif self.divtrace[5] == 'viewPageContentLabel':
                # self.divtrace[4] is the identifier; data is the human readable name.
                data = self.divtrace[4][:-2]
                self.subsectionname = data
                assert self.section is not None
                return
            elif self.divtrace[5] == 'viewPageContentData':
                pass
                if self.subsectionname in self.section:
                    self.section[self.subsectionname].append(data)
                else:
                    self.section[self.subsectionname] = [data]
                return
            else:
                print("Unknown web page part: ", self.divtrace, data)


class OrganisationParser(HTMLParser):
    def __init__(self, identifier):
        super(OrganisationParser, self).__init__()
        self.identifier = identifier
        self.divtrace = []
        self.do_trace = False
        self.is_label = False
        self.result = {'id': identifier}
        self.labelname = 'name'
        self.related_id = None

    def handle_starttag(self, tag, attrs):
        if tag == 'div':
            d = dict(attrs)
            id_ = d.get('id', '')
            class_ = d.get('class', '')
            if id_ == 'main-block':
                self.do_trace = True
            elif self.do_trace:
                trace = id_ if id_ else class_
                self.divtrace.append(trace)
        elif tag in ('label', 'h3'):
            self.is_label = True
        elif self.do_trace and tag == 'a':
            d = dict(attrs)
            url = d.get('href', '')
            if url.startswith('/meril/view/organisationUnits/'):
                identifier = int(url[30:])
                if identifier != self.identifier:
                    self.related_id = identifier

    def handle_endtag(self, tag):
        if tag == 'div' and self.do_trace:
            try:
                self.divtrace.pop()
            except IndexError:
                # pass
                self.do_trace = False
        elif tag in ('label', 'h3'):
            self.is_label = False
        elif self.do_trace and tag == 'a':
            pass
            # self.related_id = None
    
    def handle_data(self, data):
        if self.do_trace:
            # data = data.strip()
            data = re.sub('\s+', ' ', data).strip()
            # if data and self.divtrace == ['main-block', 'viewPageContentId', 'viewPageContentAccordionId', '', 'riMainSegmentContent']:
            if not data:
                return
            if self.is_label:
                if data.endswith(':'):
                    data = data[:-1]
                self.labelname = data
                return
            elif 'bread' in self.divtrace:
                return
            elif self.divtrace == ['usual1']:
                return
            elif self.divtrace == [] and 'name' not in self.result:
                self.result['name'] = data
            elif self.divtrace == [] and ('jQuery' in data or '$http.get(' in data):
                return
            elif self.divtrace[:4] == ['usual1', 'tab1', 'unPatraInfo', 'view_main_tab_subsection']:
                if self.labelname == 'URI':
                    self.result[self.labelname] = data
                else:
                    if self.labelname in self.result:
                        self.result[self.labelname].append(data)
                    else:
                        self.result[self.labelname] = [data]
            elif self.divtrace[1] == 'tab2':
                pass  # tab2 contains persons, we don't care
            elif self.divtrace[1] == 'tab3':
                pass  # tab3 contains resource infrastructure, we receive that via seperate JSON
            elif self.divtrace == ['usual1', 'tab1', '', 'firstTabSummarySections', 'summaryRelationName']:
                if self.related_id and self.labelname.startswith('Related Organization'):
                    if 'relations' in self.result:
                        self.result['relations'][self.related_id] = data
                    else:
                        self.result['relations'] = {self.related_id: data}
                    self.related_id = None
            else:
                pass
                # print(self.identifier, "Unknown web page part: ", self.divtrace, self.labelname, repr(data))



def parser_decorder_factory(parser):
    def parser_decorder(raw_data):
        # nonlocal parser
        parser.feed(raw_data)
        result = parser.result
        if not result:
            raise ValueError("Empty result after parsing with %s" % parser.__class__.__name__)
        return result
    return parser_decorder

def verify_infrastructure(identifier, infrastructure, downloader=None):
    if not isinstance(infrastructure, dict):
        raise ValueError("Infrastructure %d is not a dict" % (identifier))
    # Check if it complete
    if not infrastructure.get('incomplete', False):
        # Complete or nothing set
        try:
            _ = infrastructure['Identification']['location']
            _ = infrastructure['Structure']['typeOfRI']
            _ = infrastructure['Scientific Description']['riKeywords']
            _ = infrastructure['Classifications']['riCategory']
            _ = infrastructure['Classifications']['scientificDomain']
            infrastructure['incomplete'] = False
        except KeyError as exc:
            logging.warning("Missing key in infrastructure %d: %s" % (identifier, exc))
            infrastructure['incomplete'] = True

def verify_organisation(identifier, organisation, downloader=None):
    if not isinstance(organisation, dict):
        raise ValueError("Organisation %d is not a dict" % (identifier))
    
    relations_url = RELATIONS_URL_PATH % (identifier)
    relations_filename = 'organisationUnits/%d_facilitys.json' % (identifier)
    try:
        relations = downloader.get_cached_json(relations_url, relations_filename, ttl=20)
    except Exception as exc:
        logging.error(str(exc))
    organisation['facilitys'] = relations
    

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(levelname)-8s %(message)s')
    config = ConfigParser()
    config.read('config.ini')
    try:
        meril_folder = Path(config['MERIL']['cache_folder'])
        verify_ssl = config.getboolean('MERIL', 'verify_ssl', fallback=True)
    except KeyError:
        logging.error("Please create a configuration file 'config.ini' with section '[MERIL]' and option 'cache_folder = ./path_to_a_folder'")
        sys.exit(1)
    downloader = CachedDownloader(meril_folder)
    
    # Download organisations
    parser = SearchResultParser(BASE_MERIL_URL, ORGANISATION_RE_PATH)
    result_links = downloader.get_cached_url(
                ALL_ORGANISATIONS_URL, ALL_ORGANISATIONS_PATH,
                verify_ssl=verify_ssl, ttl=3, decode_name='search results',
                decode_func=parser_decorder_factory(parser),
                )
    logging.info("Found %d organisation links" % (len(result_links)))
    del parser

    # Download individual organisations
    organisations = {}
    json_cachefile = downloader.cachefolder / 'organisations.json'
    try:
        # Check if we already parsed them before.
        with open(json_cachefile, 'r', encoding='utf-8') as f:
            organisations_as_str = json.load(f)
            # Convert keys from string to integer
            for identifier,organisation in organisations_as_str.items():
                organisations[int(identifier)] = organisation
            del organisations_as_str
    except Exception as exc:
        logging.warning(str(exc))
        organisations = {}
    if set(organisations.keys()) != set(result_links.keys()):
        # There are new organisations. Parse them all.
        for identifier, url in result_links.items():
            html_cachefile = 'organisations_html/%d.html' % (identifier)
            parser = OrganisationParser(identifier)
            organisation = downloader.get_cached_url(
                        url, html_cachefile, 
                        verify_ssl=verify_ssl, ttl=3, decode_name='organisation HTML',
                        decode_func=parser_decorder_factory(parser),
                        )
            try:
                verify_organisation(identifier, organisation, downloader=downloader)
                organisations[identifier] = organisation
            except ValueError as exc:
                logging.error("Skip organisation %d" % (identifier))
        # Write results to file
        with open(json_cachefile, 'w', encoding='utf-8') as f:
            logging.debug("Write to %s" % (json_cachefile))
            json.dump(organisations, f, indent=1)

    # mapping from infrastructure to organisation (which we use later)
    infrastructure_organisations = {}
    for identifier,organisation in organisations.items():
        for infrastructure in organisation["facilitys"]["entities"]:
            infrastructure_id = int(infrastructure["id"])
            if infrastructure_id in infrastructure_organisations:
                infrastructure_organisations[infrastructure_id].append(identifier)
            else:
                infrastructure_organisations[infrastructure_id] = [identifier]
    
    # Download infrastructures
    parser = SearchResultParser(BASE_MERIL_URL, INFRASTRUCTURE_RE_PATH)
    result_links = downloader.get_cached_url(
                ALL_INFRASTRUCTURES_URL, ALL_INFRASTRUCTURES_PATH, 
                verify_ssl=verify_ssl, ttl=3, decode_name='search results',
                decode_func=parser_decorder_factory(parser),
                )
    logging.info("Found %d infrastructure links" % (len(result_links)))
    del parser

    # Download individual infrastructures
    infrastructures = {}
    json_cachefile = downloader.cachefolder / 'infrastructures.json'
    try:
        # Check if we already parsed them before.
        with open(json_cachefile, 'r', encoding='utf-8') as f:
            infrastructures_as_str = json.load(f)
            # Convert keys from string to integer
            for identifier, infrastructure in infrastructures_as_str.items():
                infrastructures[int(identifier)] = infrastructure
            del infrastructures_as_str
    except Exception as exc:
        logging.warning(str(exc))
        infrastructures = {}
    if set(infrastructures.keys()) != set(result_links.keys()):
        # There are new infrastructures. Parse them all.
        for identifier, url in result_links.items():
            html_cachefile = 'infrastructures_html/%d.html' % (identifier)
            parser = InfrastructureParser(identifier)
            infrastructure = downloader.get_cached_url(
                        url, html_cachefile, 
                        verify_ssl=verify_ssl, ttl=3, decode_name='infrastructure HTML',
                        decode_func=parser_decorder_factory(parser),
                        )
            assert isinstance(infrastructure, dict)
            try:
                verify_infrastructure(identifier, infrastructure, downloader=downloader)
                if identifier in infrastructure_organisations:
                    infrastructure['organisations'] = infrastructure_organisations[identifier]
                else:
                    infrastructure['organisations'] = []
                infrastructures[identifier] = infrastructure
            except ValueError as exc:
                logging.error("Skip infrastructure %d" % (identifier))
        # Write results to file
        with open(json_cachefile, 'w', encoding='utf-8') as f:
            logging.debug("Write to %s" % (json_cachefile))
            json.dump(infrastructures, f, indent=1)
    
    unlisted_infrastructures = set(infrastructure_organisations.keys()) - set(infrastructures.keys())
    if unlisted_infrastructures:
        logging.warning("Infrastructures of organisations, not listed as research infrastructure: %s" \
                         % (', '.join([str(identifier) for identifier in unlisted_infrastructures])))

