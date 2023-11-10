#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

import json
import re
import csv
from datetime import datetime

import requests


def get_ad_archive_id(data):
    """
    Extract ad_archive_id from ad_snapshot_url
    """
    return re.search(r"/\?id=([0-9]+)", data["ad_snapshot_url"]).group(1)


class FbAdsLibraryTraversal:
    default_url_pattern = (
        "https://graph.facebook.com/{}/ads_archive?access_token={}&"
        + "fields={}&search_terms={}&ad_reached_countries={}&search_page_ids={}&"
        + "ad_active_status={}&limit={}"
    )
    default_api_version = "v14.0"

    def __init__(
        self,
        access_token,
        fields,
        search_term,
        country,
        search_page_ids="",
        ad_active_status="ALL",
        after_date="1970-01-01",
        page_limit=500,
        api_version=None,
        retry_limit=3,
    ):
        self.page_count = 0
        self.access_token = access_token
        self.fields = fields
        self.search_term = search_term
        self.country = country
        self.after_date = after_date
        self.search_page_ids = search_page_ids
        self.ad_active_status = ad_active_status
        self.page_limit = page_limit
        self.retry_limit = retry_limit
        if api_version is None:
            self.api_version = self.default_api_version
        else:
            self.api_version = api_version

    def generate_ad_archives(self):
        next_page_url = self.default_url_pattern.format(
            self.api_version,
            self.access_token,
            self.fields,
            self.search_term,
            self.country,
            self.search_page_ids,
            self.ad_active_status,
            self.page_limit,
        )
        return self.__class__._get_ad_archives_from_url(
            next_page_url, after_date=self.after_date, retry_limit=self.retry_limit
        )

    @staticmethod
    def _get_ad_archives_from_url(
        next_page_url, after_date="1970-01-01", retry_limit=3
    ):
        last_error_url = None
        last_retry_count = 0
        start_time_cutoff_after = datetime.strptime(after_date, "%Y-%m-%d").timestamp()

        while next_page_url is not None:
            response = requests.get(next_page_url)
            response_data = json.loads(response.text)
            if "error" in response_data:
                if next_page_url == last_error_url:
                    # failed again
                    if last_retry_count >= retry_limit:
                        raise Exception(
                            "Error message: [{}], failed on URL: [{}]".format(
                                json.dumps(response_data["error"]), next_page_url
                            )
                        )
                else:
                    last_error_url = next_page_url
                    last_retry_count = 0
                last_retry_count += 1
                continue

            filtered = list(
                filter(
                    lambda ad_archive: ("ad_delivery_start_time" in ad_archive)
                    and (
                        datetime.strptime(
                            ad_archive["ad_delivery_start_time"], "%Y-%m-%d"
                        ).timestamp()
                        >= start_time_cutoff_after
                    ),
                    response_data["data"],
                )
            )
            if len(filtered) == 0:
                # if no data after the after_date, break
                next_page_url = None
                break
            yield filtered

            if "paging" in response_data:
                next_page_url = response_data["paging"]["next"]
            else:
                next_page_url = None

    @classmethod
    def generate_ad_archives_from_url(cls, failure_url, after_date="1970-01-01"):
        """
        if we failed from error, later we can just continue from the last failure url
        """
        return cls._get_ad_archives_from_url(failure_url, after_date=after_date)

# Insert the necessary information
access_token = "your_access_token_here"
fields = "ad_snapshot_url, ad_delivery_start_time, ad_creative_bodies, page_name, media_type, spend"
search_term = "your_search_term_here"
country = "your_country_here"

# Initializing the class
ads_traversal = FbAdsLibraryTraversal(
    access_token = "EAAKG8zItOUoBOw7FlGqdAwi4CAnjoLdiT9jD2GOLCGJyPH4ML3kuL4NMi5ZB5ZAq8d0IKdCZAWiNwZAem2XF5Hb5OTZAFiVrXlVQZCRkaJvfRLXAYAwEE3H2dAga77fx6XMZAOqJVNzfZCPiVFq4Cody59GbP9ZC2nRRLY8IvLL6Gaz5UGDmb8WGdUUxogqq46dnErxtp5Dt8HGcukeTZAwXgzrbuZBZAZB2NsIKlkY6WxdeqRKvcuPAeEDwP4pb3GwV0stdxFEqGOrnkKGcZD",
    fields=fields,
    search_term= "Liberal Alliance København",
    country= "DK",
    ad_active_status="ALL",
    after_date="2020-01-12",
    page_limit=200
)

csv_filename = 'ad_data.csv'

# Prepare the data to be written to the CSV file
data_to_write = []

csv_filename = 'ad_data.csv'

# Open the CSV file in write mode and write the header
with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
    csv_writer = csv.writer(csvfile)
    
    # Write the header row with the field names
    csv_writer.writerow(['Ad ID'] + fields.split(", "))

    # Iterate through the ad archive data and write to the CSV file
    for ad_archive_data in ads_traversal.generate_ad_archives():
        for ad in ad_archive_data:
            ad_id = get_ad_archive_id(ad)
            row_data = [ad_id]

            # Extract and append field values to the row data
            for field in fields.split(", "):
                field_value = ad.get(field)
                row_data.append(field_value)

            # Write the row to the CSV file
            csv_writer.writerow(row_data)

print(f"Data has been saved to {csv_filename}")


