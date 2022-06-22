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
from aiohttp import client_exceptions
from lxml import etree
import elementpath
import json
import progressbar
import logging


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

class Browser(object):
    def __init__(self, urls, xpaths, headers=None):
       self.urls = urls
       self.xpaths = xpaths
       self.results = dict()
       self.failed_urls = list()
       self.headers = headers

    @retry(delay=10, tries=3)
    async def get(self, session, url):
        resp = await session.get(url)
        return await resp.text()

    def xpath(self, html, xpath):
        doc = etree.HTML(html)
        if args.xpath2:
            res = elementpath.select(doc, xpath)
        else:
            res = doc.xpath(xpath)
        if res and not isinstance(res[0], etree._ElementUnicodeResult) and not isinstance(res[0], str):
            res = [r.text for r in res]
        return res

    async def parse(self, session, url, bar=None):
        try:
            html = await self.get(session, url)
            self.results[url] = {k:self.xpath(html, v) for k, v in self.xpaths.items()}
        except (etree.XPathEvalError, elementpath.exceptions.ElementPathTypeError, UnicodeDecodeError, client_exceptions.ServerDisconnectedError) as e:
            logging.error(e)
            self.failed_urls.append(url)

        if bar:
            bar.update(len(self.results))

        if args.debug:
            print(url)
            print(self.results[url])
            print(html)

        if args.interval:
            time.sleep(args.interval)


    async def harvest(self):
        cache = SQLiteBackend(cache_name='cache.db', expire_after=-1)
        async with CachedSession(cache=cache) as session:
            if(self.headers):
                session.headers.update(self.headers)
            if args.progress:
                bar = progressbar.ProgressBar(max_value=len(self.urls))
            else:
                bar = None
            tasks = [asyncio.create_task(self.parse(session, url, bar)) for url in self.urls]
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

    def dump_failed(self, filename):
        filename.write("\n".join(self.failed_urls))


async def main():
    b = Browser(args.urls, args.xpaths, args.headers)
    await b.harvest()
    if args.json or args.out.name.endswith('.json'):
        b.json(args.out)
    elif args.tab or args.out.name.endswith('.tsv'):
        b.tabular(args.out)
    elif args.prefix:
        with open(args.prefix + '.json', 'w') as f:
            b.json(f)
        with open(args.prefix + '.tsv', 'w') as f:
            b.tabular(f)
    else:
        b.tabular(args.out)

    if b.failed_urls:
        if args.prefix:
            prefix = args.prefix
        elif args.out:
            prefix = args.out.name
        with open(prefix + '.log', 'w') as f:
            b.dump_failed(f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='xpath extractor.')
    parser.add_argument('urls', nargs='+', type=str, help="URL.")
    parser.add_argument('--xpaths', nargs='+', action=keyvalue, help=" name:xpath")
    parser.add_argument('--headers', nargs='+', action=keyvalue, help="headers")
    parser.add_argument('--json', action='store_true', help="output json format")
    parser.add_argument('--xpath2', action='store_true', help="use xpath 2.0")
    parser.add_argument('--debug', action='store_true', help="show more info")
    parser.add_argument('--tab', action='store_true', help="output tabluar format")
    parser.add_argument('--progress', action='store_true', help="show progressbar")
    parser.add_argument('--interval', type=float, help="wait for seconds")
    parser.add_argument('--out', type=argparse.FileType('w'), default=sys.stdout, help="filename")
    parser.add_argument('--prefix', help="file prefix")
    
    args = parser.parse_args()

    asyncio.run(main())
