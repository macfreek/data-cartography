#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Helper functions for cached downloads.

Written by Freek Dijkstra in March 2018.

Available under Apache 2 license.
"""

from urllib.parse import urlparse
from urllib.error import HTTPError
from pathlib import Path
from os.path import join, dirname, abspath
# from exceptions import FileNotFoundError
import os
import shutil
import datetime
import json
import xml.etree.ElementTree as ET
import logging
import re
import time
import csv
from typing import List, Dict, Any
# external library
try:
    import requests
except ImportError:
    raise ImportError("Package requests is not available. Install using e.g. "
            "`port install py-requests` or `pip install requests`.") from None

# type hints
from http.client import HTTPResponse # noqa (only used for type hints)
URL = str


def get_tsv(path: str, 
            encoding='utf-8', 
            dialect='excel-tab', 
            header=None, 
            filter_func=lambda row: True
        ) -> List[Dict[str, Any]]:
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


def ensure_directory(path: Path, name="directory"):
    """Ensure that the given path is a readable and writable directory"""
    # if isinstance(path, str):
    #     path = Path(path)
    if not path.exists():
        try:
            os.makedirs(path)
            logging.warning("Created cache directory: %s" % (path))
        except Exception as exc:
            logging.error("Can't create cache directory %s: %s" % (path, exc))
    elif not os.access(path, os.W_OK):
        logging.error("Cache directory not writeable: %s" % (path))
    elif not os.access(path, os.R_OK):
        logging.error("Cache directory not readable: %s" % (path))
    elif not os.path.isdir(path):
        logging.error("Cache directory %s is not a directory" % (path))
    else:
        logging.debug("Cache directory set to: %s" % (path))

class CachedDownloader:
    def __init__(self, cachefolder: Path) -> None:
        # TODO: if not cachefolder: Create one in /var/tmp
        self.currentdir = Path(__file__).parent
        try:
            self.cachefolder = cachefolder.resolve()
        except FileNotFoundError:
            self.cachefolder = cachefolder
        ensure_directory(self.cachefolder)
        self.session = requests.Session()

    def add_cookie(self, name, value, domain, path='/'):
        self.session.cookies.set(name, value, domain=domain, path=path)

    def backup(self, sourcefile: Path) -> None:
        """Make a copy of the given file to the cachefolder"""
        try:
            sourcepath = sourcefile.resolve()  # strict=True only introduced in 3.6
            if not sourcepath.exists():
                raise FileNotFoundError("File not found: %s" % (sourcepath))
        except FileNotFoundError:
            logging.warning("Can't make backup. File not found: %s" % sourcefile)
            raise
        separator = ' ' if ' ' in sourcepath.stem else '.'
        dest_filename = sourcefile.stem + separator + datetime.date.today().isoformat() \
                    + sourcefile.suffix
        logging.debug("Backup '%s' to '%s'." % (sourcefile, dest_filename))
        
        destpath = self.cachefolder / dest_filename
        try:
            shutil.copyfile(str(sourcepath), str(destpath))
        except (OSError):
            logging.warning("Can't make backup to %s" % (destpath))
            raise

    def _url_to_short_filename(self, url: str, extension: str='.json') -> str:
        """Get filename from path and query parameters name *id or *ids.
        does not include the hostname."""
        pu = urlparse(url)
        short_name = pu.path.strip('/')
        short_name = re.sub(r'[^A-Za-z0-9]+', '_', short_name)
        if short_name.endswith("_json"):
            short_name = short_name[:-5]
        for query in pu.query.split('&'):
            try:
                k, v = query.split('=')
                if k.endswith('id') or k.endswith('ids'):
                    # replace sequence of non-word characters with _
                    v = re.sub(r'[^A-Za-z0-9]+', '_', v)
                    short_name += '_' + k + '_' + v
            except (ValueError, IndexError):
                pass  # ignore any errors
        short_name += extension
        return short_name
    
    def get_uncached_url(self, url, cookies={}, binary_mode=False, verify_ssl=True):
        logging.debug("Fetching %s" % (url))
        r = self.session.get(url, cookies=cookies, verify=verify_ssl)
        if binary_mode:
            data = r.content
        else:
            data = r.text
        return data
    
    def get_cached_url(self, 
                url: URL, 
                cache_name: str=None, 
                ttl: float=1.2, 
                cookies: dict={}, 
                encoding='utf-8', 
                decode_func=lambda x: x, 
                decode_name='text', 
                binary_mode=False, 
                verify_ssl=True
            ):
        """Return a Python object from URL or cache file.
        The ttl is time-to-live of the cache file in days."""
        if not cache_name:
            cache_name = self._url_to_short_filename(url)
        file_path = self.cachefolder / cache_name

        _downloaded_data = False
        data = None
        finalurl = url
        
        def file_age(file_path) -> float:
            """Return file age in days"""
            return (time.time() - file_path.stat().st_mtime) / 86400
        
        if file_path.exists() and (ttl is None or file_age(file_path) < ttl):
            # file exists and is recent (<28 hours)
            logging.debug("Fetching %s" % (cache_name))
            if binary_mode:
                with file_path.open('rb') as f:
                    data = f.read()
            else:
                with file_path.open('r', encoding=encoding) as f:
                    data = f.read()
        else:
            logging.debug("Fetching %s" % (url))
            try:
                r = self.session.get(url, cookies=cookies, verify=verify_ssl)
                if binary_mode:
                    data = r.content
                else:
                    data = r.text
                finalurl = r.url
                if finalurl != url:
                    logging.info("%s redirects to %s." % (url, finalurl))
                r.raise_for_status()
                _downloaded_data = True
            except requests.ConnectionError as exc:
                logging.error("Can't connect to %s: %s" % (url, exc))
                raise ConnectionError("Failed to download data from %s" % url) from None
            except (HTTPError, requests.exceptions.HTTPError) as e:
                logging.warning("HTTP error for %s: %s" % (url, e))
                # Regretfully, in case of HTTP Error 429: Too Many Requests,
                # e.headers does not contain a "Retry-after" header on all sites.
                raise ConnectionError("Failed to download data from %s" % url) from None
        
        try:
            decoded_data = decode_func(data)
        except ValueError as e:
            if finalurl == url:
                logging.error("Can't decode %s from %s: %s" % (url, decode_name, e))
                raise ValueError("Failed to download data from %s" % url) from None
            elif 'login' in finalurl:
                logging.error("%s redirects to non-%s login page %s. " \
                              "Please verify login credentials." % (url, decode_name, finalurl))
                raise PermissionError("Redirected to login page from %s" % url) from None
            else:
                logging.error("%s redirects to non-%s page %s." % (url, decode_name, finalurl))
                raise ConnectionError("Redirected to non-%s page from %s" % (decode_name, url)) \
                        from None
        if _downloaded_data:
            try:
                logging.debug("Write to %s" % (file_path))
                if binary_mode:
                    with file_path.open('wb') as f:
                        f.write(data)
                else:
                    with file_path.open('w', encoding=encoding) as f:
                        f.write(data)
            except OSError as e:
                logging.warning("%s" % (e))
                # report and proceed (ignore missing cache)
        return decoded_data
    
    def get_cached_json(self, url: URL, cache_name: str=None, ttl: float=1.2, cookies: dict={}):
        """Return a Python object from URL or cache file.
        The ttl is time-to-live of the cache file in days."""
        return self.get_cached_url(url, cache_name, ttl, cookies=cookies, 
                        decode_func=json.loads, decode_name='JSON', binary_mode=False)

    def get_cached_xml(self, url: URL, cache_name: str=None, ttl: float=1.2, cookies: dict={}):
        """Return a Python object from URL or cache file.
        The ttl is time-to-live of the cache file in days."""
        return self.get_cached_url(url, cache_name, ttl, cookies=cookies, 
                        decode_func=ET.XML, decode_name='XML', binary_mode=False)

    def get_cached_binary(self, url: URL, cache_name: str=None, ttl: float=1.2, cookies: dict={}):
        """Return a Python object from URL or cache file.
        The ttl is time-to-live of the cache file in days."""
        return self.get_cached_url(url, cache_name, ttl, cookies=cookies, 
                        decode_name='Image', binary_mode=True)
