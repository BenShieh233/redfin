from lxml import html
import requests
import argparse
import json

def parse(address):
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, sdch, br',
        'accept-language': 'en-GB,en;q=0.8,en-US;q=0.6,ml;q=0.4',
        'cache-control': 'max-age=0',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
    }
    response = requests.get(
        f"https://www.redfin.com/stingray/do/location-autocomplete?location={address}&start=0&count=10&v=2", headers=headers)

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter
    )
    argparser.add_argument('3434 E Woodbine Rd, Orange, CA 92867', help='')
    args = argparser.parse_args()
    print(args)
