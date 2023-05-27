import asyncio
import json
import math
from typing import List, Literal, Optional, TypedDict

import jmespath
from scrapfly import ScrapeConfig, ScrapeApiResponse, ScrapflyClient

scrapfly = ScrapflyClient(key="YOUR SCRAPFLY KEY", max_concurrency=2)


class PropertyResult(TypedDict):
    listing_id: str
    title: str
    description: str
    url: str
    price: str
    photos: List[dict]
    ...  # and much more


def parse_property(result: ScrapeApiResponse) -> Optional[PropertyResult]:
    data = extract_next_data(result)
    if not data:
        return
    result = jmespath.search(
        """listingDetails.{
        id: listingId,
        title: title,
        description: detailedDescription,
        url: listingUris.detail,
        price: pricing.label,
        type: propertyType,
        date: publishedOn,
        category: category,
        section: section,
        features: features.bullets,
        floor_plan: floorPlan.image.{filename:filename, caption: caption}, 
        nearby: pointsOfInterest[].{title: title, distance: distanceMiles},
        coordinates: location.coordinates.{lat:latitude, lng: longitude},
        photos: propertyImage[].{filename: filename, caption: caption},
        details: analyticsTaxonomy,
        agency: branch
    }""", data)
    return result


def extract_next_data(result: ScrapeApiResponse) -> dict:
    """find hidden next.js data in page scrape result"""
    data = result.selector.css("script#__NEXT_DATA__::text").get()
    if not data:
        print(f"page {result.context['url']} is not a property listing page")
        return
    data = json.loads(data)
    return data["props"]["pageProps"]


async def scrape_properties(urls: List[str]) -> List[PropertyResult]:
    """scrape Zoopla property pages and parse results"""
    to_scrape = [ScrapeConfig(url=url, asp=True, country="GB") for url in urls]
    properties = []
    async for result in scrapfly.concurrent_scrape(to_scrape):
        properties.append(parse_property(result))
    return properties


async def find_properties(query: str, query_type: Literal["for-sale", "to-rent"] = "for-sale"):
    """scrape Zooplas search system and find all search results"""
    # scrape first results page to start:
    first_page = await scrapfly.async_scrape(
        ScrapeConfig(
            url=f"https://www.zoopla.co.uk/search/?view_type=list&section={query_type}&q={query}&geo_autocomplete_identifier=&search_source=home",
            country="GB",
            asp=True,
        )
    )
    # extract next.js data and the listings of the first page
    data = extract_next_data(first_page)["initialProps"]["searchResults"]
    listings = data["listings"]["regular"]
    # then extract total pages
    total_results = data["pagination"]["totalResults"]
    total_pages = math.ceil(data["pagination"]["totalResults"] / len(listings))
    if total_pages > data["pagination"]["pageNumberMax"]:
        total_pages = data["pagination"]["pageNumberMax"]

    # scrape reamining pages concurrently:
    print(f"total {total_results} results, {total_pages} pages")
    other_pages = [ScrapeConfig(url=first_page.context["url"] + f"&pn={page}") for page in range(2, total_pages + 1)]
    async for result in scrapfly.concurrent_scrape(other_pages):
        data = extract_next_data(result)["initialProps"]["searchResults"]
        listings.extend(data["listings"]["regular"])
    return listings
