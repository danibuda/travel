from .scrape import WebPage, ProxyList
from urllib.parse import urlparse
import re
try:
    import sys
    if 'threading' in sys.modules:
        del sys.modules['threading']
        print('threading module loaded before patching!')
        print('threading module deleted from sys.modules!\n')
    from gevent import monkey, pool
except ImportError:
    print("Gevent is not installed. Parsing process will be slower.")
    gevent_installed = False
else:
    monkey.patch_all()
    gevent_installed = True


class Crawler:
    """Crawl url and generate sitemap"""
    def __init__(self, url, outputfile="output.xml", logfile="error.log", oformat="xml", echo=True):
        self.url = url
        self.logfile = open(logfile, "a")
        self.oformat = oformat
        self.outputfile = "sitemaps\\" + outputfile
        self.pool = []
        self.echo = echo
        self.regex = None

        # create lists for the urls in que and visited urls
        self.proxy_list = ProxyList()
        self.urls = set([url])
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
        setattr(self, "regex", re.compile(self.allowed_regex))
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
        if re.search(self.regex, url):
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
