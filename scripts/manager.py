import googlemaps
from random import randint
from unidecode import unidecode
from json import dumps
from math import floor
from time import sleep, time
import datetime
from slugify import slugify
from .scrape import ProxyList, WebPage, download_file
from .util import hash_factors, mk_dir
from os import listdir
from os.path import basename, splitext, isfile, join, exists
from database.mappings import *
from scripts.exceptions import ResourceError
from urllib.parse import urlsplit, parse_qs


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
        if place_details["status"] == "OK":
            return place_details["result"]
        else:
            return {}


class ChurchManager:
    """Parse and Scrape one domain"""
    def __init__(self, access, connection, domain, path):
        self.access = access
        self.connection = connection
        self.domain = domain
        self.path = path
        self.items = []
        self.proxy_list = ProxyList()
        self.download = download_file
        self.maps = MapClient(access, timeout=5, queries_per_second=100, language='ro')

    def index(self):
        page = WebPage(self.domain + "/index.php?menu=BI")
        self.items = page.match_elements("area",
                                         {"shape": "circle"},
                                         ["href", ("title", "(?:\s*-\s*)(.*)(?:\s*biserici)")])

    def dump(self):
        if not len(self.items):
            self.index()
        for item in self.items:
            default_county = self.connection.get(County, {"code": item[0].replace("/index.php?menu=BI", "")}, "match")
            default_church_count = int(item[1].replace(".", ""))
            existing_church_count = len(self.connection.index(Church, None, {"county_id": default_county.id}, "match"))
            if default_church_count == existing_church_count:
                continue
            keys = []
            for i in range(0, int(floor(default_church_count/20)+1)):
                page = WebPage(self.domain + item[0] + "&start={}&order=".format(i*20))
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

    def scrape(self, scrape_flag=None):
        churches = self.connection.index(Church, None, {"telephone": "new", "address": None}, "match")
        for church in churches:
            new_values = {}
            church_page = WebPage(self.domain + church.url, self.proxy_list)
            try:
                if church_page.status_code != 200:
                    new_values.update({"geo_hash_id": "error"})
                    print("{} - url error, status: {}".format(self.domain + church.url, church_page.status_code))
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
                                                           {"name": city_name, "county_id": church.county_id},
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

                        new_values.update({"address": dumps(address, ensure_ascii=False).encode("utf8")})

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
                                   slugify(church.church_county.name) + "\\" +\
                                   slugify(church.church_city.name) + "\\" +\
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
                                    self.download(img, img_path + "\\" + str(k) + ext.lower(), self.proxy_list)
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


class ScrapeManager:
    """Parse and Scrape one domain"""
    def __init__(self, access, connection, path):
        self.access = access
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

    def scrape(self, scrape_flag="images"):
        counties = self.connection.index(County)
        for county in counties:
            churches = self.connection.index(Church, None, {"county_id": county.id, "address": None}, "match")
            new_values = {}
            for church in churches:
                church_page = WebPage(church.url, False)
                try:
                    if len(church_page.soup.find_all("table")) == 0:
                        new_values.update({"address": "url_error"})
                        self.connection.put(Church, church.id, new_values)
                    else:
                        try:
                            if church.photo_cnt != 0 and church.photo_cnt != 1 and scrape_flag == "images":
                                img_path = self.path +\
                                                     "\\" +\
                                                     slugify(church.church_city.name) +\
                                                     "\\" + \
                                                     slugify(church.church_city.name + " " + church.title)
                                church_imgs = []
                                for img in church_page.get_images():
                                    if "zz_" in img:
                                        church_imgs.append(img.replace(church.url, "").replace("zz_", ""))
                                if exists(img_path):
                                    current_files = [f for f in listdir(img_path) if isfile(join(img_path, f))]
                                else:
                                    current_files = []
                                if len(current_files) == len(church_imgs):
                                    continue
                                else:
                                    if len(church_imgs) > 10:
                                        church_imgs = church_imgs[:10]
                                    mk_dir(img_path)
                                    for k, img in enumerate(church_imgs):
                                        filename = basename(img)
                                        ext = splitext(filename)[1] if splitext(filename)[1] else ".jpg"
                                        self.download(img, img_path + "\\" + str(k) + ext.lower())
                                        sleep(randint(2, 4))
                            if not church.address:
                                address_fields = ["Localitate:", "Comună:", "Judeţ:", "Adresa:", "Cod poştal:"]
                                interest_fields = ["Telefon :", "Adresă de e-mail :", "Detalii:"]
                                contact, address = {}, {}
                                page_table_rows = church_page.soup.find_all("tr")
                                for row in page_table_rows[1:]:
                                    columns = row.find_all("td")
                                    if len(columns) < 2:
                                        continue
                                    first_cell = columns[0].get_text()
                                    second_cell = columns[1].get_text()
                                    if first_cell in address_fields:
                                        address.update({slugify(first_cell): str(second_cell)})
                                    if first_cell in interest_fields and "NU deţinem" not in second_cell:
                                        if first_cell is "Telefon :":
                                            new_values.update({"telephone": str(second_cell)})
                                        contact.update({slugify(first_cell): second_cell})
                                new_values.update({"address": dumps(address, ensure_ascii=False).encode("utf8")})
                                self.connection.put(Church, church.id, new_values)
                        except AttributeError:
                            print(church.id)
                        sleep(randint(3, 7))
                except AttributeError:
                    new_values.update({"address": "error"})
                    self.connection.put(Church, church.id, new_values)

    def validate(self):
        pass
