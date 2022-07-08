import atexit
import time
import os
import pdb
import argparse
from urllib.parse import urlparse
import sys
from retry import retry
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, NoSuchElementException, ElementNotInteractableException
from selenium.webdriver.common.keys import Keys
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
    def __init__(self, urls, xpaths, cookies=None):
       self.urls = urls
       self.xpaths = xpaths
       self.results = dict()
       self.failed_urls = list()
       self.cookies = cookies
       self.done = list()
       self.driver = self.connect_to_remote_driver(args.host, args.port, args.driver_options)

    def connect_to_remote_driver(self, host, port, options={}):
        options = Options(**options)
        options.add_experimental_option('w3c', True)

        driver = webdriver.Remote(
           command_executor='http://{host}:{port}/wd/hub'.format(host=host, port=port),
           desired_capabilities=DesiredCapabilities.CHROME,
           options=options)
        return driver

    @retry(delay=10, tries=3)
    def get(self, url):
        self.driver.get(url)
        if self.cookies:
            self.driver.add_cookie(self.cookies)

    def xpath(self, html, xpath):
        doc = etree.HTML(html)
        if args.xpath2:
            res = elementpath.select(doc, xpath)
        else:
            res = doc.xpath(xpath)
        if res and not isinstance(res[0], etree._ElementUnicodeResult) and not isinstance(res[0], str):
            res = [r.text for r in res]
        return res

    def parse(self, session, url, bar=None):
        try:
            html = self.get(session, url)
            self.results[url] = {k:self.xpath(html, v) for k, v in self.xpaths.items()}
            if args.additional_info:
                self.results[url].update(args.additional_info)
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

    def do(self, actions):
        ele = self.driver
        for action_to_parse in actions:
            act, arg = action_to_parse.split(':', 1)
            print(act)
            if act.startswith('find_element'):
                ele = getattr(ele, act)(By.XPATH, arg)
            else:
                ele = getattr(ele, act)()

            if not self.is_element_exists(ele) or ele is None:
                ele = self.driver


    def is_element_exists(self, ele):
        try:
            ele.text
        except StaleElementReferenceException as e:
            return False
        return True

    def harvest(self):
        with progressbar.ProgressBar(max_value=len(self.urls)) as bar:
            for i, url in enumerate(self.urls):
                bar.update(i)
                self.get(url)
                self.do(args.actions)

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

@atexit.register
def clean():
    if 'b' in globals():
        b.driver.close()

def dump_results(b):
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

def main():
    b.harvest()
    if b.results:
        dump_results(b)

    if b.failed_urls:
        if args.prefix:
            prefix = args.prefix
        elif args.out:
            prefix = args.out.name
        with open(prefix + '.log', 'w') as f:
            b.dump_failed(f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='selenium xpath extractor.')
    parser.add_argument('urls', nargs='+', type=str, help="URL.")
    parser.add_argument('--xpaths', nargs='+', action=keyvalue, help=" name:xpath")
    parser.add_argument('--actions', nargs='+', default=dict(), help=" action:xpath")
    parser.add_argument('--cookies', nargs='+', action=keyvalue, help="cookies")
    parser.add_argument('--additional_info', nargs='+', action=keyvalue, help="headers")
    parser.add_argument('--json', action='store_true', help="output json format")
    parser.add_argument('--host', help="remote ip")
    parser.add_argument('--port', default=4444, help="remote port")
    parser.add_argument('--driver_options', default=dict(), help="driver options")
    parser.add_argument('--debug', action='store_true', help="show more info")
    parser.add_argument('--tab', action='store_true', help="output tabluar format")
    parser.add_argument('--progress', action='store_true', help="show progressbar")
    parser.add_argument('--interval', type=float, help="wait for seconds")
    parser.add_argument('--out', type=argparse.FileType('w'), default=sys.stdout, help="filename")
    parser.add_argument('--prefix', help="file prefix")
    
    args = parser.parse_args()
    b = Browser(args.urls, args.xpaths, args.cookies)

    main()
