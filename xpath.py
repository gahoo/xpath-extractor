import atexit
import time
import os
import pdb
import argparse
from urllib.parse import urlparse
import sys
from retry import retry
import asyncio
from aiohttp_client_cache import CachedSession, SQLiteBackend
from lxml import etree
import json


class keyvalue(argparse.Action):
    # Constructor calling
    def __call__( self , parser, namespace,
                 values, option_string = None):
        setattr(namespace, self.dest, dict())
          
        for value in values:
            # split it into key and value
            key, value = value.split(':', 1)
            # assign into dictionary
            getattr(namespace, self.dest)[key] = value

parser = argparse.ArgumentParser(description='xpath extractor.')
parser.add_argument('urls', nargs='+', type=str, help="URL.")
parser.add_argument('--xpaths', nargs='+', action=keyvalue, help=" name:xpath")
parser.add_argument('--headers', nargs='+', action=keyvalue, help="headers")
parser.add_argument('--json', action='store_true', help="output json format")
parser.add_argument('--tab', action='store_true', help="output tabluar format")
parser.add_argument('--out', type=argparse.FileType('w'), default=sys.stdout, help="wait in seconds")

args = parser.parse_args()

class Browser(object):
    def __init__(self, urls, xpaths, headers=None):
       self.urls = urls
       self.xpaths = xpaths
       self.results = dict()
       if(headers):
           self.session.headers.update(headers)

    @retry(delay=10, tries=3)
    async def get(self, session, url):
        resp = await session.get(url)
        return await resp.text()

    def xpath(self, html, xpath):
        doc = etree.HTML(html)
        res = doc.xpath(xpath)
        if res and not isinstance(res[0], etree._ElementUnicodeResult):
            res = [r.text for r in res]
        return res

    async def parse(self, session, url):
        html = await self.get(session, url)
        self.results[url] = {k:self.xpath(html, v) for k, v in self.xpaths.items()}


    async def harvest(self):
        cache = SQLiteBackend(cache_name='cache.db', expire_after=-1)
        async with CachedSession(cache=cache) as session:
            tasks = [asyncio.create_task(self.parse(session, url)) for url in self.urls]
            return await asyncio.gather(*tasks)

    def json(self, filename):
        filename.write(json.dumps(self.results, ensure_ascii = False))

    def tabular(self, filename):
        header = 'url' + '\t' + "\t".join(self.xpaths.keys())
        content = [header]
        for url, xpaths in self.results.items():
            extracted = ["; ".join(filter(lambda x:x, v)) for v in xpaths.values()]
            content.append(url + "\t" + "\t".join(extracted))
        filename.write("\n".join(content))


async def main():
    b = Browser(args.urls, args.xpaths, args.headers)
    await b.harvest()
    if args.json:
        b.json(args.out)
    if args.tab:
        b.tabular(args.out)

if __name__ == '__main__':
    asyncio.run(main())
