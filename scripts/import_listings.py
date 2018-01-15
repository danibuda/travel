from database.mappings import *
from os.path import basename, splitext
from .util import import_csv, mk_dir, hash_factors, similarity
from .scrape import ListingPage, GoogleResultPage
from google_images_download import google_images_download
import time, random, datetime, re
from slugify import slugify

root_path = "C:\\Users\\fabbs\\Google Drive\\Mapping Romania"
platform = re.compile(r"^(?:https?:\/\/)?(?:www\.)?((?:[\w|-]+\.)*[\w|-]+)(?:\.[a-z]+)")
price = re.compile(r"(\d+(?:[\.|,]\d+)?)")

def import_booking_listings(connection, logging, csv_path, map_client, priority_counties = []):
    listing_types = ["B&B", "Casa", "Vila", "Barca", "Cabana", "Han", "Hotel", "Motel", "Pensiune",
                     "Chalet", "Hostel", "Complex", "Camping", "Apartament", "Camera de inchiriat"]
    csv_listing, listing_len = import_csv(csv_path)
    hotel_ids = []
    existing_booking = []
    if len(priority_counties):
        for priority_county in priority_counties:
            county_listings = connection.index(BookingListing, None, {"county_id": int(priority_county)}, "match")
            existing_booking.extend(county_listings)
    for each_booking in existing_booking:
        hotel_ids.append(int(each_booking.hotel_id))
    if len(existing_booking)!= len(hotel_ids):
        logging.warning("Duplicate hotel found!")
        exit()
    del existing_booking
    del each_booking
    cnt = 0
    while cnt <= listing_len:
        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        try:
            if ".ro." in csv_listing['URL'][cnt]:
                listing_url = csv_listing['URL'][cnt]
            else:
                listing_url = csv_listing['URL'][cnt].replace(".html", ".ro.html")
            listing_city = slugify(csv_listing["City"][cnt])
            listing_title = " ".join(x.capitalize() if x else ' ' for x in slugify(csv_listing['Hotels'][cnt]).split("-"))
            listing = BookingListing(hotel_id = csv_listing["Hotel ID"][cnt], title = listing_title)
        except Exception as e :
            print(e)
            cnt += 1
            continue

        if int(listing.hotel_id) in hotel_ids:
            cnt += 1
            continue
        try:
            city = connection.get(City, {"name" : listing_city}, "like")
            if city is None:
                city = connection.get(City, {"name": listing_city.replace("-", " ")}, "like")
        except Exception:
            city = None
            logging.warning("City not found")
        try:
            listing.listing_city = city
            listing.city_id = city.id
            listing.listing_county = city.county
            listing.county_id = city.county.id
            listing.photos = "\\Booking\\" + slugify(city.county.name) + "\\" + city.name + " " + listing_title
        except AttributeError:
            pass
        # if len(priority_counties) and listing.listing_city.county.code not in priority_counties:
        #     cnt += 1
        #     continue
        try:
            listing_page = ListingPage(listing_url)
            listing_details = listing_page.parse_booking_page()
            listing_microformats = listing_page.get_microformats()
        except Exception as e:
            logging.critical("Error getting listing data, hotel_id : %d", int(listing.hotel_id))
            cnt += 1
            continue
        finally:
            listing.update({"geo_hash_id": hash_factors(listing_details["latitude"],
                                                        listing_details["longitude"],
                                                        listing.hotel_id),
                            "latitude": listing_details["latitude"],
                            "longitude": listing_details["longitude"],
                            "address" : listing_details["longitude"]})

        try:
            avg_price = price.search(listing_microformats["priceRange"]).group(0)
            listing.address = listing_microformats["address"]["streetAddress"]
            #check check yo
            nearby_places = map_client.get_nearby_places((listing.latitude, listing.longitude), 10)
            try:
                for place_near in nearby_places["results"]:
                    title_similarity = similarity(str(place_near["name"]), str(listing.title))
                    if title_similarity > 0.8:
                        place_details = map_client.set_place_details(place_near["place_id"])
                        listing.place_id = place_details["place_id"]
                        new_platform = BookingListingPlatform(geo_hash_id = listing.geo_hash_id,
                                                              platform = platform.search(place_details["url"]).group(1),
                                                              url = place_details["url"],
                                                              last_modified = timestamp)
                        listing.platforms.append(new_platform)
                        listing.telephone = place_details["formatted_phone_number"]
            except KeyError:
                logging.warning("Error getting places nearby, hotel_id : %d", int(listing.hotel_id))
        except KeyError:
            logging.warning("Error with listing microformat, hotel_id : %d", int(listing.hotel_id))
        except Exception as e:
            print(e)
        finally:
            new_platform = BookingListingPlatform(geo_hash_id =listing.geo_hash_id,
                                                  platform =platform.search(listing_url).group(1),
                                                  url = listing_url,
                                                  avg_price = avg_price,
                                                  last_modified = timestamp)
            listing.platforms.append(new_platform)
        listing_google_page = GoogleResultPage(str(listing.title + " " + city.name)).listing_prices()
        for result in listing_google_page:
            try:
                new_platform = BookingListingPlatform(geo_hash_id = listing.geo_hash_id,
                                                      platform = platform.search(result[0]).group(1),
                                                      url = result[0],
                                                      avg_price = result[1],
                                                      last_modified = timestamp)
                listing.platforms.append(new_platform)
            except AttributeError:
                pass
        for key in listing_details.keys():
            if key in ["facilities", "capacity", "vecinity"]:
                listing_facility = BookingListingFacility(geo_hash_id = listing.geo_hash_id,
                                                          type = key,
                                                          content = listing_details[key])
                listing.facilities.append(listing_facility)
            elif key in ["short_description", "long_description"]:
                listing_description = BookingListingContent(geo_hash_id = listing.geo_hash_id,
                                                            type = key,
                                                            src_url = listing_url,
                                                            content = listing_details[key])
                listing.contents.append(listing_description)
            elif key == "images":
                pass
                # try:
                #     if len(listing["images"]):
                #         images = listing['images']
                #         del listing['images']
                # except KeyError:
                #     logging.critical("No images found, hotel_id %d", int(listing["hotel_id"]))
                #     pass
                # listing_path = mk_dir(root_path + listing.photos)
                # downloader = google_images_download.Downloader()
                # for k, item in enumerate(images):
                #     filename = basename(item)
                #     ext = splitext(filename)[1] if splitext(filename)[1] else ".jpg"
                #     downloader.run(item = item, filename = listing_path + "\\" + str(k) + ext.lower())
                #     time.sleep(random.randrange(15, 20))
                # time.sleep(random.randrange(20, 30))


    # for platform in booking_listing.booking_listing_platform_collection:
    #     if platform.platform == "booking":
    #         booking_website = platform.url
    #         break
    # booking_website = re.sub("\?.*$", "", booking_website)
    # booking_listing_page = download_page(booking_website)
    # booking_listing_page_title = get_page_title(booking_listing_page)
    # booking_listing_page_title = re.sub("\s*\(.*\).*$", "", booking_listing_page_title)
    # for type in listing_types:
    #     if type in booking_listing_page_title:
    #         booking_listing.title_ro = booking_listing_page_title
    #         booking_listing.type = type
    #         updated_fields.extend(["type", "title_ro"])
    #         break



        connection.post(BookingListing, listing)
        hotel_ids.append(listing.hotel_id)
        logging.info("Inserted new accomodation %d", int(listing.hotel_id))
        cnt += 1