#!/usr/bin/env python3

from html.parser import HTMLParser
import re
import os
from os.path import basename, dirname, abspath, join, splitext
import json
import sys
import logging



class InfrastructureParser(HTMLParser):
    def __init__(self, identifier=None):
        super(InfrastructureParser, self).__init__()
        self.identifier = identifier
        self.divtrace = []
        self.do_trace = False
        self.entity = {'id': int(identifier)}
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
                self.entity['name'] = data
                return
            elif self.divtrace == ['viewPageHeaderId', 'viewPageRIURLId', 'viewPageRIURLLinkId']:
                self.entity['url'] = data
                return
            elif self.divtrace == ['viewPageContentId', 'viewPageContentAccordionId', '', 'riHorizontalHeader', 'riHorizontalHeaderLabel']:
                self.sectionname = data
                self.section = {}
                self.entity[data] = self.section
                return
            elif self.divtrace == ['viewPageContentId', 'viewPageContentAccordionId', '', 'riMainSegmentContent']:
                # if self.subsectionname is not None and self.subsectionname not in self.section:
                #     print(self.identifier, "Found subsection without content: ", self.subsectionname)
                #     print(self.entity)
                self.subsectionname = data
                assert self.section is not None
                return
            elif self.divtrace[:6] == ['viewPageContentId', 'viewPageContentAccordionId', '', 'riMainSegmentContent', 'customAccordionPanel', 'viewPageContentDataV2']:
                assert self.section is not None
                if self.subsectionname is None:
                    print (self.identifier, "Found subsection without section name: ", data)
                    self.subsectionname = ''
                # assert self.subsectionname is not None
                if self.subsectionname in self.section:
                    self.section[self.subsectionname].append(data)
                else:
                    self.section[self.subsectionname] = [data]
                return
            elif self.divtrace == ['viewPageContentId']:
                if data == 'Information for this RI entry is currently being completed':
                    self.entity['incomplete'] = True
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
    def __init__(self, identifier=None):
        super(OrganisationParser, self).__init__()
        self.identifier = identifier
        self.divtrace = []
        self.do_trace = False
        self.is_label = False
        self.entity = {'id': identifier}
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
            elif self.divtrace == [] and 'name' not in self.entity:
                self.entity['name'] = data
            elif self.divtrace == [] and ('jQuery' in data or '$http.get(' in data):
                return
            elif self.divtrace[:4] == ['usual1', 'tab1', 'unPatraInfo', 'view_main_tab_subsection']:
                if self.labelname == 'URI':
                    self.entity[self.labelname] = data
                else:
                    if self.labelname in self.entity:
                        self.entity[self.labelname].append(data)
                    else:
                        self.entity[self.labelname] = [data]
            elif self.divtrace[1] == 'tab2':
                pass  # tab2 contains persons, we don't care
            elif self.divtrace[1] == 'tab3':
                pass  # tab3 contains resource infrastructure, we receive that via seperate JSON
            elif self.divtrace == ['usual1', 'tab1', '', 'firstTabSummarySections', 'summaryRelationName']:
                if self.related_id and self.labelname.startswith('Related Organization'):
                    if 'relations' in self.entity:
                        self.entity['relations'][self.related_id] = data
                    else:
                        self.entity['relations'] = {self.related_id: data}
                    self.related_id = None
            else:
                pass
                # print(self.identifier, "Unknown web page part: ", self.divtrace, self.labelname, repr(data))


def parse_file(srcpath, dstdir, ParserClass, callback_funcs=[]):
    identifier = splitext(basename(srcpath))[0]
    parser = ParserClass(int(identifier))
    try:
        with open(srcpath, 'r', encoding='utf-8') as f:
            data = f.read()
    except Exception as exc:
        print(exc)
        return
    parser.feed(data)
    entity = parser.entity
    if callback_funcs:
        for callback in callback_funcs:
            callback(entity)
    if dstdir is None:
        return entity
    dstpath = join(dstdir, identifier + '.json')
    with open(dstpath, 'w', encoding='utf-8') as f:
        json.dump(parser.entity, f)


def convert_dir_to_json(srcdir, dstdir, ParserClass, callbacks=[]):
    srcdir = abspath(join(dirname(__file__),srcdir))
    dstdir = abspath(join(dirname(__file__),dstdir))
    for fn in os.listdir(srcdir):
        logging.debug("Processing %s" % fn)
        srcpath = join(srcdir, fn)
        parse_file(srcpath, dstdir, ParserClass, callbacks)

def ensure_dict_keys(**key_values):
    def ensure_dict_items(entity):
        for key, value in key_values.items():
            if key not in entity:
                entity[key] = value
    return ensure_dict_items

def insert_other_json_from_to(srcdir, keyname):
    def insert_other_json(entity):
        filename = '%d_facilitys.json' % (entity['id'])
        relations_path = abspath(join(dirname(__file__), srcdir, filename))
        try:
            with open(relations_path, 'r', encoding='utf-8') as f:
                data = f.read()
                data = json.loads(data)
        except Exception as exc:
            logging.error("Error loading %s: %s" % (filename, exc))
            return
        entity[keyname] = data
    return insert_other_json

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(levelname)-8s %(message)s')
    # parse_file('./infrastructures_html/14477.html', None, InfrastructureParser)
    # parse_file(abspath(join(dirname(__file__),'organisations_html/135950.html')), 'organisations', OrganisationParser)
    callbacks = [ensure_dict_keys(incomplete=False)]
    convert_dir_to_json('infrastructures_html', 'infrastructures', InfrastructureParser, callbacks)
    callbacks = [insert_other_json_from_to('organisationUnits', 'facilitys')]
    convert_dir_to_json('organisations_html', 'organisations', OrganisationParser, callbacks)

