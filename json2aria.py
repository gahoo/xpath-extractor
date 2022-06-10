import os
import pdb
import argparse
import sys
import json
from jinja2 import Template

def extname(x):
    return os.path.splitext(x)[1]

def parse(data):
    entries = []
    for url, entry in data.items():
        each_page = template.render(extname=extname, dirname=os.path.dirname, zip=zip, prefix=args.prefix, len=len, set=set,
            url = url, **entry)
        entries.append(each_page)
    return "".join(entries)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='xpath extractor.')
    parser.add_argument('json', type=argparse.FileType('r'), help="json files")
    parser.add_argument('--out', type=argparse.FileType('w'), default=sys.stdout, help="filename")
    parser.add_argument('--template_file', type=argparse.FileType('r'), help="filename")
    parser.add_argument('--template_string', help="aria2 string")
    parser.add_argument('--prefix', default='.', help="")

    args = parser.parse_args()

    data = json.load(args.json)
    if args.template_string:
        template = Template(args.template_string)
    elif args.template_file:
        template = Template(args.template_file.read())

    results = parse(data)
    args.out.write(results)
