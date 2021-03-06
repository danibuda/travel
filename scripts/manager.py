import googlemaps
import datetime
import errno
from random import randint
from unidecode import unidecode
from json import dumps, loads
from math import floor
from re import compile
from time import sleep, time
from slugify import slugify
from .scrape import ProxyList, WebPage, download_file
from .util import hash_factors, mk_dir
from os import listdir, makedirs
from os.path import basename, splitext, isfile, join, exists, dirname
from database.mappings import *
from scripts.exceptions import *
from urllib.parse import urlsplit, parse_qs
from math import ceil
from PIL import Image


def reflect_table(keys, values):
    rez = {}
    for k, v in enumerate(values):
        key = keys[k]
        text = v.get_text().replace("&nbsp", "")
        try:
            try:
                link = v.find("a").attrs["href"]
            except AttributeError:
                pass
            else:
                rez.update({key: (text, link)})
                continue
            title = v.attrs["title"]
        except KeyError:
            title = None
        rez.update({key: (text, title)})
    return rez


class MapClient:

    def __init__(self, access, **kwargs):
        self.client = googlemaps.Client(key=access.place_api_key)
        for key in kwargs:
            setattr(self, key, kwargs[key])

    def perform_request(self, params=None):
        """This method will perform the place request and return the request object."""
        if params is None:
            return None
        if not self.client:
            raise ResourceError("Error, no client found.")
        sleep(2)
        results = {}
        try:
            results = self.client.places(**params)
        except Exception as e:
            print(e)
        return results

    def deal_with_pagination(self, params, data):
        """Perform multiple calls in order to have a full list of elements when the results are "paginated"."""
        all_data = []
        while data["next_page_token"]:
            if data["results"]:
                all_data.extend(data["results"])
            if data["next_page_token"]:
                params["page_token"] = data["next_page_token"]
            data = self.perform_request(params)
        return all_data

    def get_places(self, params=None):
        """
            In case of success the method will return the content of the response to the request.
            Pagination is automatically detected and handled accordingly
        """
        if params is None:
            return None
        try:
            req = self.perform_request(params)
            if req["status"] == "ZERO_RESULTS":
                return []
            if req["next_page_token"]:
                return self.deal_with_pagination(params, req)
            else:
                return req["results"]
        except Exception as e:
            print(e)
            return []

    def find_location(self, lat, long):
        if not self.client:
            raise ResourceError("Error, no client found.")
        return self.client.reverse_geocode((lat, long), language='ro')

    def find_address(self, address):
        if not self.client:
            raise ResourceError("Error, no client found.")
        return self.client.geocode(address)

    def get_nearby_places(self, *args, **kargs):
        if not self.client:
            raise ResourceError("Error, no client found.")
        return self.client.places_nearby(*args, **kargs)

    def set_place_details(self, place_id):
        if not self.client:
            raise ResourceError("Error, no client found.")
        try:
            place_details = self.client.place(place_id, language="ro")
        except Exception as e:
            print(e)
            place_details = {}
        try:
            return place_details["result"]
        except KeyError:
            return {}


class ScrapeManager:
    """Parse and Scrape one domain"""
    def __init__(self, access, map_client, connection, path=None):
        self.access = access
        self.map_client = map_client
        self.connection = connection
        self.path = path
        self.proxy_list = ProxyList()
        self.download = download_file

    def get_page_elements(self, url, tag, attribute_match=None, attributes_returned=None):
        """
        Get a list of match element from a url.
        :param url: string, url of page to scrape
        :param tag: string, html tag to match
        :param attribute_match: dict, strings attributes and values pair to match
        :param attributes_returned: list, strings or tuples with attribute values
        :return: list, BS elements matched with filtered results
        """
        page = WebPage(url, self.proxy_list)
        return page.match_elements(tag, attribute_match, attributes_returned)

    def dump(self, items):
        for item in items:
            keys = []
            for i in range(0, int(floor(int(item[1])/20)+1)):
                page = WebPage(item[0] + "&start={}&order=".format(i*20))
                page_table = page.match_elements("table")[0]
                if len(keys) == 0:
                    for key in page_table.find_all("th"):
                        t = slugify(key.get_text())
                        if t:
                            keys.append(t)
                for row in page_table.find_all("tr"):
                    columns = row.find_all("td")
                    if len(columns):
                        church_dict = reflect_table(keys, columns[1:])
                        church = Church(title=church_dict["denumire"][0],
                                        url=church_dict["denumire"][1],
                                        religion=church_dict["religie"][0],
                                        photo_cnt=church_dict["fotografii"][0])
                        check_church = self.connection.get(Church, {"url": church_dict["denumire"][1]}, "match")
                        if not isinstance(check_church, Church):
                            cod_lmi = str(church_dict["cod-lmi"][0]) if church_dict["cod-lmi"][0] != "" else None
                            city_name = unidecode(church_dict["localitate"][0].lower())
                            county_name = unidecode(church_dict["judet"][0].lower())
                            if "Bucureşti" in county_name:
                                county_name = "ilfov"
                            if city_name == "bucuresti":
                                county_name = "bucuresti"
                            county = self.connection.get(County, {"name": county_name}, "match")
                            city = self.connection.get(City,
                                                       {"name": city_name, "county_id": county.id},
                                                       "like")
                            if church_dict["localizare"][0] == "*Biserică":
                                lat, long = church_dict["localizare"][1].replace("Coordonate: ", "").split(",")
                                geo_hash_id = hash_factors(lat, long, church.url)
                            else:
                                lat, long = None, None
                                geo_hash_id = None
                            try:
                                church.update({"cod_lmi": cod_lmi,
                                               "geo_hash_id": geo_hash_id,
                                               "latitude": lat,
                                               "longitude": long})
                            except AttributeError:
                                pass
                            try:
                                church.update({"city": city,
                                               "city_id": city.id,
                                               "county": city.county,
                                               "county_id": city.county.id})
                            except AttributeError:
                                pass
                            self.connection.post(church)

    def dump_2(self, items):
        for item in items:
            default_county = self.connection.get(County, {"code": item[0].replace("/index.php?menu=BI", "")}, "match")
            default_church_count = int(item[1].replace(".", ""))
            existing_church_count = len(self.connection.index(Church, None, {"county_id": default_county.id}, "match"))
            if default_church_count == existing_church_count:
                continue
            keys = []
            for i in range(0, int(floor(default_church_count/20)+1)):
                page = WebPage(item[0] + "&start={}&order=".format(i*20))
                page_table = page.match_elements("table")[0]
                if len(keys) == 0:
                    for key in page_table.find_all("th"):
                        t = slugify(key.get_text())
                        if t:
                            keys.append(t)
                for row in page_table.find_all("tr"):
                    columns = row.find_all("td")
                    if len(columns):
                        church_dict = reflect_table(keys, columns[1:])
                        church = Church(title=church_dict["denumire"][0],
                                        url=church_dict["denumire"][1],
                                        religion=church_dict["religie"][0],
                                        photo_cnt=church_dict["fotografii"][0])
                        check_church = self.connection.get(Church, {"url": church_dict["denumire"][1]}, "match")
                        if not isinstance(check_church, Church):
                            cod_lmi = str(church_dict["cod-lmi"][0]) if church_dict["cod-lmi"][0] != "" else None
                            city_name = unidecode(church_dict["localitate"][0].lower())
                            county_name = unidecode(church_dict["judet"][0].lower())
                            if "Bucureşti" in county_name or "ucuresti" in county_name:
                                county_name = "ilfov"
                            if city_name == "bucuresti":
                                county_name = "bucuresti"
                            county = self.connection.get(County, {"name": county_name}, "match")
                            try:
                                city = self.connection.get(City, {"name": city_name, "county_id": county.id}, "like")
                            except AttributeError:
                                city = self.connection.get(City,
                                                           {"name": city_name, "county_id": default_county.id},
                                                           "like")
                            try:
                                church.update({"city": city,
                                               "city_id": city.id,
                                               "county": city.county,
                                               "county_id": city.county.id})
                            except AttributeError:
                                pass
                            finally:
                                church.update({"telephone": "new"})

                            if church_dict["localizare"][0] == "*Biserică":
                                lat, long = church_dict["localizare"][1].replace("Coordonate: ", "").split(",")
                                try:
                                    church.update({"cod_lmi": cod_lmi,
                                                   "geo_hash_id": hash_factors(lat, long, church.url),
                                                   "latitude": lat,
                                                   "longitude": long})
                                except AttributeError:
                                    pass

                            self.connection.post(church)

    def parse(self):
        timestamp = datetime.datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')
        interest_points = self.connection.index(InterestPoint, {"address_coord": None})
        for int_point in interest_points:
            new_values = {}
            contact = {}
            if int_point.website:
                contact.update({"website": int_point.website})
                new_values.update({"website": None})
                platform = PLTFRM.search(int_point.website).group(1)
                self.connection.post(InterestPointPlatform(geo_hash_id=int_point.geo_hash_id,
                                                           platform=platform,
                                                           url=int_point.website,
                                                           last_modified=timestamp))
            if int_point.telephone:
                new_values.update({"telephone": None})
                contact.update({"telephone": int_point.telephone})

            if len(contact):
                contact = dumps(contact, ensure_ascii=False).encode("utf8")
                self.connection.post(InterestPointFacility(geo_hash_id=int_point.geo_hash_id,
                                                           type="contact",
                                                           content=contact))

            if int_point.place_id:
                new_address = {}
                place_details = self.map_client.set_place_details(int_point.place_id)
                try:
                    for component in place_details["address_components"]:
                        if component["types"][0] in ["route", "locality", "administrative_area_level_2",
                                                     "administrative_area_level_1", "country"]:
                            new_address.update({component["types"][0]: component["short_name"]})
                except KeyError:
                    pass

                new_values.update({"address_coord": dumps(new_address, ensure_ascii=False).encode("utf8"),
                                   "address": None})

            if not int_point.address:
                addresses = self.map_client.find_location(int_point.latitude, int_point.longitude)
                try:
                    for address in addresses:
                        new_address = {}
                        for component in address["address_components"]:
                            if component["types"][0] in ["route", "locality", "administrative_area_level_2",
                                                         "administrative_area_level_1", "country"]:
                                new_address.update({component["types"][0]: component["short_name"]})
                        packed_address = dumps(new_address, ensure_ascii=False)

                        if int_point.city.name in packed_address and int_point.city.county.code in packed_address:
                            new_values.update({"address_coord": packed_address.encode("utf8")})
                            break
                except KeyError:
                    pass
            if len(new_values):
                self.connection.put(InterestPoint, int_point.id, new_values)
            # exit()

    def scrape_2(self, scrape_flag=None):
        churches = self.connection.index(Church, None, {"telephone": "new", "address": None}, "match")
        for church in churches:
            new_values = {}
            church_page = WebPage(church.url, self.proxy_list)
            try:
                if church_page.status_code != 200:
                    new_values.update({"geo_hash_id": "error"})
                    print("{} - url error, status: {}".format(church.url,
                                                              church_page.status_code))
                else:
                    if not church.address or not church.city_id:
                        address_fields = ["Localitate:", "Comună:", "Judeţ:", "Adresa:", "Cod poştal:"]
                        interest_fields = ["Telefon :", "Adresă de e-mail :", "Detalii:"]
                        contact, address = {}, {}
                        page_table_rows = church_page.soup.find_all("tr")
                        for row in list(reversed(page_table_rows[1:])):
                            columns = row.find_all("td")
                            if len(columns) < 2:
                                continue
                            first_cell = columns[0].get_text()
                            second_cell = columns[1].get_text()

                            if first_cell == "Judeţ:":
                                judet = self.connection.get(County, {"name": slugify(second_cell)})
                                try:
                                    new_values.update({"county_id": judet.id})
                                except AttributeError:
                                    print("{} - no matching county was found".format(second_cell))

                            if first_cell == "Localitate:":
                                city_name = second_cell.split("-")[0] + "-"
                                oras = self.connection.get(City,
                                                           {"name": city_name,
                                                            "county_id": church.county_id},
                                                           "like")
                                try:
                                    new_values.update({"city_id": oras.id})
                                except AttributeError:
                                    print("{} - no matching city was found".format(second_cell))

                            if first_cell in address_fields:
                                address.update({slugify(first_cell): str(second_cell)})

                            if first_cell in interest_fields and "NU deţinem" not in second_cell:
                                if first_cell is "Telefon :":
                                    new_values.update({"telephone": str(second_cell)})
                                contact.update({slugify(first_cell): second_cell})

                        new_values.update(
                            {"address": dumps(address, ensure_ascii=False).encode("utf8")})

                    if not church.geo_hash_id:
                        maps_url = church_page.soup.find("a", class_="fancybox-google")
                        try:
                            params = parse_qs(urlsplit(maps_url.attrs["href"]).query)
                            lat, long = params["q"][0].split(",")
                            new_values.update({"latitude": lat,
                                               "longitude": long,
                                               "geo_hash_id": hash_factors(lat, long, church.url)})
                        except AttributeError:
                            continue

                    self.connection.put(Church, church.id, new_values)

                    if church.photo_cnt >= 2 and scrape_flag == "images":
                        img_path = self.path + "\\" + \
                                   slugify(church.church_county.name) + "\\" + \
                                   slugify(church.church_city.name) + "\\" + \
                                   church.church_city.name + " - " + church.title
                        # img_path = self.path + "\\" + slugify(church.church_city.name) +\
                        #            "\\" + slugify(church.church_city.name + " " + church.title)
                        church_imgs = []
                        for img in church_page.get_images():
                            if "zz_" in img:
                                church_imgs.append(img.replace(church.url, "").replace("zz_", ""))
                        if exists(img_path):
                            current_files = [f for f in listdir(img_path) if isfile(join(img_path, f))]
                        else:
                            current_files = []
                        if len(current_files):
                            continue
                        else:
                            if len(church_imgs) > 10:
                                church_imgs = church_imgs[:10]
                            mk_dir(img_path)
                            for k, img in enumerate(church_imgs):
                                filename = basename(img)
                                ext = splitext(filename)[1] if splitext(filename)[1] else ".jpg"
                                try:
                                    self.download(img, img_path + "\\" + str(k) + ext.lower(),
                                                  self.proxy_list)
                                except Exception as e:
                                    print(e)
                                else:
                                    sleep(randint(2, 4))

            except AttributeError:
                continue

    def transfer_table(self):
        timestamp = datetime.datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')
        counties = self.connection.index(County)
        for county in counties:
            churches = self.connection.index(Church, None, {"county_id": county.id, "telephone": "fixed"}, "match")
            for church in churches:
                interest_count = 0
                geo_hash_id = hash_factors(church.longitude, church.latitude, church.url)
                if church.geo_hash_id != geo_hash_id:
                    self.connection.put(Church, church.id, {"geo_hash_id": geo_hash_id, "telephone": "fixed"})
                interest_match = {"latitude": church.latitude, "longitude": church.longitude, "city_id": church.city_id}
                interest_points = self.connection.index(InterestPoint, None, interest_match, "match")
                if len(interest_points):
                    for int_p in interest_points:
                        if int_p.platforms[0].url == "biserici.org" + church.url:
                            interest_count += 1
                            if int_p.geo_hash_id != geo_hash_id:
                                self.connection.put(InterestPoint, int_p.id, {"geo_hash_id": geo_hash_id})
                                self.connection.put(Church, church.id, {"telephone": "moved"})
                    if interest_count > 1:
                        print("{} duplicates found of geo_hash_id: {} ".format(interest_count, geo_hash_id))
                else:
                    i_p = InterestPoint(geo_hash_id=geo_hash_id,
                                        title=church.title,
                                        city_id=church.city_id,
                                        latitude=church.latitude,
                                        longitude=church.longitude,
                                        types="place_of_worship",
                                        address_coord=church.address,
                                        status="invalid" if int(church.photo_cnt) < 2 else None,
                                        telephone=church.telephone)
                    i_p.platforms.append(InterestPointPlatform(geo_hash_id=geo_hash_id,
                                                               platform="biserici",
                                                               url="biserici.org" + church.url,
                                                               last_modified=timestamp))
                    info = dumps({"religion": church.religion,
                                  "cod_lmi": None if not church.cod_lmi else church.cod_lmi,
                                  "photo_count": church.photo_cnt}, ensure_ascii=False).encode("utf8")
                    i_p.facilities.append(InterestPointFacility(geo_hash_id=geo_hash_id, type="info", content=info))
                    self.connection.post(i_p)
                    self.connection.put(Church, church.id, {"telephone": "moved"})

    def fix_images(self, folder_path, new_path):
        if not folder_path:
            raise Exception("No path specified!")

        counties = self.connection.index(County)
        for county in counties:
            county_folder = slugify(county.name)
            cities = self.connection.index(City, {"county_id": county.id})
            county_folder = join(folder_path, county_folder)
            for city in cities:
                city_folder = slugify(city.name)
                city_folder = join(county_folder, city_folder)
                interests = self.connection.index(InterestPoint, {"city_id": city.id, "types": "place_of_worship"})
                for interest in interests:
                    move_flag = False
                    info = loads(interest.facilities[0].content)

                    if info["photo_count"] < 2:
                        if interest.status != "invalid":
                            self.connection.put(InterestPoint, interest.id, {"status": "invalid"})
                        continue

                    output_path = join(new_path, slugify(county.name),
                                       slugify(city.name),
                                       city.name + " - " + interest.title)
                    interest_folder = join(city_folder, city.name + " - " + interest.title)

                    old_struc_folder = join(r"C:\Users\fabbs\Desktop\Churches_2",
                                            slugify(city.name),
                                            slugify(city.name + " " + interest.title))
                    if not exists(interest_folder) or not len(listdir(interest_folder)):
                        if not exists(old_struc_folder):
                            try:
                                print(" No img folder found for interest {}: {} !".format(interest.title, interest.id))
                                self.connection.put(InterestPoint, interest.id, {"status": "no_imgs"})
                            except Exception as e:
                                print(e)
                            continue
                        interest_folder, old_struc_folder = old_struc_folder, interest_folder
                        move_flag = True

                    non_cropped = 0
                    for img_file in listdir(interest_folder):
                        try:
                            if "Cazare" in img_file:
                                filename = img_file
                            else:
                                filename = "Cazare_" + city.name.title() + "_" + img_file
                            im = Image.open(join(interest_folder, img_file))

                            if move_flag:
                                new_img_path = join(old_struc_folder,  filename)
                                if not exists(dirname(new_img_path)):
                                    try:
                                        makedirs(dirname(new_img_path))
                                    except OSError as exc:  # Guard against race condition
                                        if exc.errno != errno.EEXIST:
                                            raise
                                im.save(new_img_path)

                            self.resize_and_crop(im, join(output_path, filename))

                        except Exception as e:
                            print(e)
                    if non_cropped >= info["photo_count"] - 1:
                        self.connection.put(InterestPoint, interest.id, {"status": "invalid_imgs"})
                    else:
                        self.connection.put(InterestPoint,
                                            interest.id,
                                            {"photos": join(slugify(county.name),
                                                            slugify(city.name),
                                                            city.name + " - " + interest.title)})
                    # exit()

    def resize_and_crop(self, im, output_path, crop_size=(800, 600), limit_size=(500, 500)):
        """
        Resize and crop images from center.
        :param im: sampled image
        :param output_path: complete folder and file path
        :param crop_size: tuple with dimensions of crop box
        :param limit_size: tuple with size limit of sample image
        """

        try:
            sample_im = im
            initial_size = im.size

            if initial_size[0] <= limit_size[0] or initial_size[1] <= limit_size[1]:
                return

            resize = False
            ratio = initial_size[0] / initial_size[1]

            width_delta = ceil(ratio * (initial_size[0] - crop_size[0]))
            height_delta = ceil(ratio * (initial_size[1] - crop_size[1]))

            if width_delta < 0 or height_delta < 0:
                resize = True

            if ratio < 1:
                resize_box = (initial_size[0] - 2*width_delta, ceil((initial_size[0] - 2*width_delta)/ratio))
            elif ratio > 1:
                resize_box = (ceil((initial_size[1] - 2*width_delta)*ratio), initial_size[1] - 2*height_delta)
            elif ratio == 1:
                resize_box = (initial_size[0] + width_delta, initial_size[1] + height_delta)

            if resize:
                sample_im = sample_im.resize(resize_box, Image.ANTIALIAS)

            width, height = sample_im.size

            cropped_im = sample_im.crop(((width - crop_size[0]) / 2, (height - crop_size[1]) / 2,
                                         (width + crop_size[0]) / 2, (height + crop_size[1]) / 2))
            if not exists(dirname(output_path)):
                try:
                    makedirs(dirname(output_path))
                except OSError as exc:  # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise

            cropped_im.save(output_path)
        except Exception as e:
            print(e)


PLTFRM = compile(r"^(?:https?://)?(?:www\.)?((?:[\w|-]+\.)*[\w|-]+)(?:\.[a-z]+)")
