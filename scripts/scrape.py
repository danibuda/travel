import requests
import string
import pickle
import os
import time
from hashlib import md5
from json import dumps, loads
from .util import convert_coordinates
from slugify import slugify
from random import choice
from fake_useragent import UserAgent
from urllib.parse import quote, urlparse
from requests.exceptions import ProxyError, ConnectTimeout, ReadTimeout
from urllib3 import Timeout
from bs4 import BeautifulSoup, element
from re import compile, search, findall, sub, I
from scripts.exceptions import ResourceError

try:
    import sys
    if 'threading' in sys.modules:
        del sys.modules['threading']
    from gevent import monkey, pool
except ImportError:
    print("Gevent is not installed. Parsing process will be slower.")
    gevent_installed = False
else:
    monkey.patch_all()
    gevent_installed = True


def find_price(tag):
    price = None
    if not isinstance(tag, element.NavigableString):
        try:
            for child in tag.contents:
                child_price = find_price(child)
                if child_price:
                    price = child_price
                    break
        except AttributeError:
            pass
    else:
        match_prices = PRC_PTN.search(repr(tag))
        try:
            price = match_prices.group()
        except AttributeError:
            pass
    return price


def download_file(url, file_path, proxy=None, timeout=Timeout(connect=5, read=20)):
        if os.path.isfile(file_path):
            print('{} - file already exists'.format(file_path))
            return
        headers = {'user-agent': UserAgent().chrome}
        if proxy is not None:
            while True:
                proxy_host = proxy.random()
                try:
                    response = requests.get(url,
                                            headers=headers,
                                            proxies={"http": proxy_host, "https": proxy_host},
                                            stream=True,
                                            timeout=timeout)
                except (ProxyError, ConnectTimeout, ReadTimeout):
                    continue
                else:
                    break
        else:
            response = requests.get(url, headers=headers, stream=True, timeout=timeout)
        try:
            response.raise_for_status()
            with open(file_path, 'wb') as output_file:
                for block in response.iter_content(1024):
                    if not block:
                        break
                    output_file.write(block)
        except IOError:
            print("IOError on image {}".format(file_path))
        except Exception as e:
            print("Error {}".format(e))
        return file_path


class ProxyList:
    """Crawl url and generate sitemap"""
    def __init__(self, file=r"assets\proxies", logfile="proxy_list_errors.log"):
        self.checked = set()
        try:
            if os.stat(file).st_mtime < time.time() - 3 * 86400:
                os.remove(file)
            with open(file, 'rb') as f:
                self.checked = pickle.load(f)
        except FileNotFoundError:
            self.logfile = open("logs\\" + logfile, "a")
            self.queue = set()
            break_flag = False
            for i in range(0, 10):
                ssl = False
                if i % 2:
                    response = requests.get("http://pubproxy.com/api/proxy?limit=20&format=txt&type=https")
                    ssl = True
                else:
                    response = requests.get("http://pubproxy.com/api/proxy?limit=20&format=txt&type=http")

                for item in response.text.split("\n"):
                    if "We have to temporarily stop you" in item:
                        break_flag = True
                        break
                    else:
                        self.queue.update(["{}://{}".format("https" if ssl else "http", item)])

                if break_flag:
                    break

            response = requests.get("https://raw.githubusercontent.com/stamparm/aux/master/fetch-some-list.txt")
            for item in loads(response.text):
                try:
                    if item["proto"] not in ["socks4", "socks5"]:
                        self.queue.update(["{}://{}:{}".format(item["proto"], item["ip"], item["port"])])
                except KeyError:
                    pass

            response = requests.get("https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list.txt")
            for line, item in enumerate(response.text.split("\n")):
                if 5 < line < 305:
                    proxy_info = item.split(" ")
                    try:
                        proxy = "{}://{}".format("https" if proxy_info[1].endswith("S") else "http", proxy_info[0])
                        self.queue.update([proxy])
                    except Exception as e:
                        print(e)

            if gevent_installed:
                self.pool = pool.Pool(10)
                self.pool.spawn(self.parse_gevent)
                self.pool.join()
            else:
                while len(self.queue) > 0:
                    self.parse()

            with open(file, "wb") as f:
                pickle.dump(self.checked, f)

    def is_valid(self, proxy_host, timeout=Timeout(connect=5, read=10)):
        try:
            response = requests.get("https://canihazip.com/s", proxies={"http": proxy_host, "https": proxy_host},
                                    timeout=timeout)
        except Exception as e:
            raise ProxyError(e)
        else:
            if response.text != proxy_host.replace("http://", "").split(":")[0]:
                raise ProxyError("Proxy check failed: {} not used while requesting".format(proxy_host))
        self.checked.update([proxy_host])

    def parse_gevent(self):
        self.parse()
        while len(self.queue) > 0 and not self.pool.full():
            self.pool.spawn(self.parse_gevent)

    def parse(self):
        if not gevent_installed:
            print("{} proxies parsed :: {} proxies in the queue".format(len(self.checked), len(self.queue)))
        else:
            print("{} proxies parsed :: {} parsing processes :: {} proxies in the queue".format(len(self.checked),
                                                                                                len(self.pool),
                                                                                                len(self.queue)))
        if not len(self.queue):
            return
        else:
            proxy = self.queue.pop()
            if proxy in self.checked:
                return
            try:
                self.is_valid(proxy)
            except Exception as e:
                self.errlog(str(e))

    def random(self):
        if len(self.checked):
            return choice(self.checked)
        else:
            raise ResourceError

    def errlog(self, msg):
        self.logfile.write(msg)
        self.logfile.write("\n")


class Crawler:
    """Crawl url and generate sitemap"""
    def __init__(self, url, outputfile="output.xml", logfile="crawler_errors.log", oformat="xml", echo=True):
        self.url = url
        self.logfile = open("logs\\" + logfile, "a")
        self.oformat = oformat
        self.outputfile = "sitemaps\\" + outputfile
        self.pool = []
        self.echo = echo
        self.regex = None

        # create lists for the urls in que and visited urls
        self.proxy_list = ProxyList()
        self.urls = {[url]}
        self.visited = set()
        self.exts = ["htm", "php"]
        self.allowed_regex = "\.((?!htm)(?!php)\w+)$"

    def set_exts(self, exts):
        self.exts = exts

    def allow_regex(self, regex=None):
        if regex is not None:
            self.allowed_regex = regex
        else:
            allowed_regex = ''
            for ext in self.exts:
                allowed_regex += '(!{})'.format(ext)
            self.allowed_regex = '\.({}\w+)$'.format(allowed_regex)

    def crawl(self, pool_size=1):
        setattr(self, "regex", compile(self.allowed_regex))
        if gevent_installed and pool_size >= 1:
            self.pool = pool.Pool(pool_size)
            self.pool.spawn(self.parse_gevent)
            self.pool.join()
        else:
            while len(self.urls) > 0:
                self.parse()
        if self.oformat == 'xml':
            self.write_xml()
        elif self.oformat == 'txt':
            self.write_txt()

    def parse_gevent(self):
        self.parse()
        while len(self.urls) > 0 and not self.pool.full():
            self.pool.spawn(self.parse_gevent)

    def parse(self):
        if self.echo:
            if not gevent_installed:
                print("{} pages parsed :: {} pages in the queue".format(len(self.visited), len(self.urls)))
            else:
                print("{} pages parsed :: {} parsing processes :: {} pages in the queue".format(len(self.visited),
                                                                                                len(self.pool),
                                                                                                len(self.urls)))
        # Set the starting point for the spider
        if not len(self.urls):
            return
        else:
            url = self.urls.pop()
            if url in self.visited:
                return
            try:
                response = WebPage(url, self.proxy_list)
                if response.status_code > 301:
                    self.errlog("Error {} at url {}".format(response.status_code, url))
                    return
                for link in [x.replace("../", self.url) for x in response.get_links()]:
                    if self.is_valid(link):
                        self.urls.update([link])
            except Exception as e:
                self.errlog(str(e))
            finally:
                self.visited.update([url])

    def is_valid(self, url):
        parsed_url = urlparse(url)
        if "#" in url:
            url = url[:url.find("#")]
        if url.count(".") > 3:
            return False
        if not parsed_url.netloc or parsed_url.netloc not in self.url:
            return False
        if search(self.regex, url):
            return False
        return True

    def errlog(self, msg):
        self.logfile.write(msg)
        self.logfile.write("\n")

    def write_xml(self):
        of = open(self.outputfile, "w")
        of.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n")
        of.write("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\""
                 " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
                 " xsi:schemaLocation=\"http://www.sitemaps.org/schemas/sitemap/0.9"
                 " http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd\">\n")
        url_str = "<url><loc>{}</loc></url>\n"
        while len(self.visited):
            of.write(url_str.format(self.visited.pop().encode("utf-8")))

        of.write("</urlset>")
        of.close()

    def write_txt(self):
        of = open(self.outputfile, "w")
        url_str = "{}\n"
        while len(self.visited):
            of.write(url_str.format(self.visited.pop()))

        of.close()


class WebPage:
    """Generic webpage"""
    def __init__(self, url, proxylist=None, timeout=Timeout(connect=5, read=20)):
        self.url = url
        headers = {'user-agent':  UserAgent().chrome}
        if proxylist is not None:
            while True:
                proxy_host = proxylist.random()
                try:
                    res = requests.get(url,
                                       headers=headers,
                                       proxies={"http": proxy_host, "https": proxy_host},
                                       timeout=timeout)
                except (ProxyError, ConnectTimeout, ReadTimeout):
                    continue
                else:
                    break
        else:
            res = requests.get(url, headers=headers, timeout=timeout)
        self.status_code = res.status_code
        try:
            response = res.content
            # response_encoding = detect(response)
            # if response_encoding["encoding"] != "UTF-8" and response_encoding["confidence"] > 0.7:
            #     response = response.decode("Windows-1250")
        except Exception as e:
            print(e)
            self.soup = None
        else:
            self.soup = BeautifulSoup(response, "lxml")

    def match_elements(self, tag, attribute_match=None, attributes_returned=None):
        result = []
        try:
            if "class" in attribute_match.keys():
                attribute_match["class_"] = attribute_match["class"]
                del attribute_match["class"]
            collection = self.soup.find_all(tag, **attribute_match)
        except AttributeError:
            collection = self.soup.find_all(tag)
        for matched_tag in collection:
            try:
                item = ()
                for att_re in attributes_returned:
                    try:
                        item += (matched_tag.attrs[att_re],)
                    except KeyError:
                        item += (search(att_re[1], matched_tag.attrs[att_re[0]].strip()).group(1),)
                result.append(item)
            except TypeError:
                result.append(matched_tag)
        return result

    def page_content(self):
        results = {}
        if not self.soup:
            return results
        translator = str.maketrans('', '', string.punctuation)
        for span_tag in self.soup.find_all(['span', 'i', 'b']):
            span_tag_text = ' '.join(span_tag.text.split())
            span_tag_text_clean = span_tag_text.translate(translator)
            if len(span_tag_text) - len(span_tag_text_clean) < 2:
                span_tag.replace_with(span_tag_text_clean)
            else:
                span_tag.replace_with(' ')
        content_elements = self.soup.find_all(['p', 'h1', 'h2', 'h3'])
        for content_line in content_elements:
            striped_line = ' '.join(content_line.text.split())
            if striped_line:
                cleaned_string = striped_line.translate(translator)
                if len(striped_line) - len(cleaned_string) < 15 and len(cleaned_string) > 40:
                    results.update({md5(striped_line.encode()): striped_line})
        return results

    def get_images(self):
        results = []
        for img in self.soup.find_all("img"):
            try:
                results.append(self.url + "/" + img.attrs["src"])
            except KeyError:
                pass
        return results

    def get_links(self):
        return [x.attrs["href"] for x in self.soup.find_all('a', href=True)]

    def get_title(self):
        try:
            title_tag = self.soup.find("title")
        except Exception as e:
            print(e)
            title_tag = None
        return title_tag.string.strip()

    def get_metadata(self, name):
        meta_tags = self.soup.find_all("meta")
        for meta_tag in meta_tags:
            try:
                if meta_tag.attrs['name'] == name:
                    return meta_tag.attrs['content']
            except KeyError:
                pass

    def reflect_table(self, attributes_returned=None):
        results = []
        keys = []
        page_table = self.soup.find("table")[0]
        for heading in page_table.find_all("th"):
            key = slugify(heading.get_text())
            if isinstance(key, str):
                keys.append(key)
        for row in page_table.find_all("tr"):
            columns = row.find_all("td")
            if len(columns):
                rez = {}
                for column_index, column_value in enumerate(columns):
                    key = keys[column_index]
                    value = ()
                    for attr in attributes_returned:
                        if attr == "text":
                            value += (column_value.get_text().replace("&nbsp", ""),)
                        else:
                            try:
                                value += (column_value.attrs[attr],)
                            except KeyError:
                                value += (column_value.find("a").attrs[attr],)
                    rez.update({key: value})
                results.append(rez)
        return results


class ListingPage(WebPage):
    """Listing handling"""

    def parse_booking_page(self):
        map_element = self.soup.select("a[class*=\"map_static_zoom\"]")
        try:
            coordinates = findall("\d+\.\d+", map_element[0].attrs["style"])
        except Exception:
            raise
        try:
            pagedetails = {"latitude": coordinates[0], "longitude": coordinates[1]}
        except Exception as e:
            pagedetails = {"status": "error"}
            print(e)
        try:
            address = self.soup.find("span", class_="hp_address_subtitle jq_tooltip")
            pagedetails.update({"address": address.text.strip()})
        except AttributeError:
            pass

        try:
            description = self.soup.find("div", id="summary")
            pagedetails.update({"long_description": description.text.strip().replace("\n", "")})
        except AttributeError:
            pass

        vecinity = {}
        for item in self.soup.select("div[class*=\"hp-surroundings-category\"]"):
            try:
                item_text = [sub("\s+", " ", "".join(li.text.strip())) for li in item.find_all("li")]
                vecinity.update({item.find("h3").text.strip(): item_text})
            except AttributeError:
                pass
        pagedetails.update({"vecinity": dumps(vecinity, ensure_ascii=False).encode("utf8")})

        facilities = {}
        for item in self.soup.find_all("div", class_="facilitiesChecklistSection"):
            try:
                item_text = ["".join(li.text.strip().replace("\n", "")) for li in item.find_all("li")]
                facilities.update({item.find("h5").text.strip(): item_text})
            except AttributeError:
                pass
        pagedetails.update({"facilities": dumps(facilities, ensure_ascii=False).encode("utf8")})

        capacity = []
        try:
            cap = self.soup.find("span", class_="occupancy_multiplier_number").text.strip()
        except AttributeError:
            cap = "unknown"
        for item in self.soup.find_all("li", class_="bedroom_bed_type"):
            try:
                capacity.append(''.join(item.text.strip().replace("\n", "").replace("\xa0", " ")))
            except AttributeError:
                pass
        pagedetails.update({"capacity": dumps({cap: capacity}, ensure_ascii=False).encode("utf8")})

        images = []
        for img in self.soup.find_all("img", class_="hide"):
            images.append(sub("max\d00", "max1024x768", img.attrs["src"]))
        if not len(images):
            for img in self.soup.select("a[class*=\"hotel_thumbs_sprite\"]"):
                if len(images) > 10:
                    break
                try:
                    if img.attrs["href"] != "#":
                        images.append(sub("max\d00", "max1024x768", img.attrs["href"]))
                except KeyError:
                    continue
        pagedetails.update({"images": images, "short_description": self.get_metadata("description")})
        return pagedetails

    def get_microformats(self):
        try:
            microformats_tag = self.soup.find("script", type="application/ld+json")
            microformats = loads(microformats_tag.text)
        except Exception as e:
            print(e)
            microformats = None
        return microformats


class WikiPage(WebPage):
    """ wikipedia handeling"""

    def wiki_table(self):
        rez = {}
        table = self.soup.find_all("table", class_="wikitable")[0]
        table_rows = table.find_all("tr")
        data_cnt = 0
        for row in table_rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            else:
                if "datatable" in table.attrs["class"]:
                    new = {}
                    for i, cell in enumerate(cells):
                        if i == 0:
                            new['title'] = cell.a.attrs["title"]
                            new['href'] = cell.a.attrs["href"]
                        if i == 1:
                            new['city'] = (cell.a.attrs["href"], cell.a.text.replace('\n', ''))
                        if i == 3:
                            new['type'] = cell.text.split()[0]
                    rez[data_cnt] = new
                else:
                    for i, cell in enumerate(cells):
                        if not cell.text:
                            continue
                        if i == 0:
                            county = cell.text.split()[0]
                            county_href = cell.a.attrs["href"]
                            rez[county] = county_href
                data_cnt += 1
        return rez

    def wiki_page(self):
        page_description = ""
        content_div = self.soup.find_all("div", class_="mw-content-ltr")[0]
        paragrafs = content_div.find_all("p")
        try:
            latitude = convert_coordinates(content_div.find("span", class_="latitude").text)
            longitude = convert_coordinates(content_div.find("span", class_="longitude").text)
        except AttributeError:
            latitude = None
            longitude = None
        for paragraf in paragrafs:
            page_description += paragraf.text
        return {"description": page_description, "latitude": latitude, "longitude": longitude}


class GoogleResultPage:
    """google handling"""

    def __init__(self, access, query):
        page = WebPage(access, "https://www.google.com/search?q=" + quote(query))
        self.items = page.soup.find_all("div", {"class": "rc"})

    def listing_prices(self):
        results = []
        for item in self.items:
            href = item.find("a").attrs['href']
            price = find_price(item)
            if price:
                results.append((href, price))
        return results

    def result_page(self):
        results = []
        for item in self.items:
            tag = item.find("a")
            href = tag.attrs['href']
            results.append((href, item.text.strip()))
        return results


PLTFRM = compile(r"^(?:https?://)?(?:www\.)?((?:[\w|-]+\.)*[\w|-]+)(?:\.[a-z]+)")
PRC_PTN = compile("(?i)((?:\d{2,}\s*(?:euro|lei|ron|\$|€|£))|(?:(?:euro|lei|ron|\$|€|£)\s*\d{2,}))")
LNK_PTN = compile('^(a|button)$', I)
