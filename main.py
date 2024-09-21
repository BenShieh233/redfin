from ReviewSQL import Session
from extract_first_review import extract_reviews
from scrape_ceiling_fans_with_lights import scrape_data
import json
import pandas as pd


if __name__ == '__main__':

    df = scrape_data()

    extract_reviews(df)

    