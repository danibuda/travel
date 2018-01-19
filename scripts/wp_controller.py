from wordpress_xmlrpc import Client, WordPressPost, WordPressTerm
from wordpress_xmlrpc.methods import taxonomies, posts, media
from wordpress_xmlrpc.compat import xmlrpc_client
from database.mappings import *
from scripts.exceptions import *
from os.path import basename, splitext
from phpserialize import *


class WordPressManager:
    """Manage interest points inside WordPress"""
    def __init__(self, access, connection):
        try:
            self.client = Client('https://www.travelatar.ro/xmlrpc.php', access.wp_auth["usr"], access.wp_auth["pwd"])
        except Exception as e:
            raise ResourceError(e)

        self.connection = connection
        self.post_types = self.client.call(posts.GetPostTypes())
        self.posts = {}
        self.taxonomies = {}
        self.items = []
        self.pages = []

    def index_taxonomies(self):
        for taxonomy in self.client.call(taxonomies.GetTaxonomies()):
            self.taxonomies.update({taxonomy.name: self.client.call(taxonomies.GetTerms(taxonomy.name))})

    def index_posts(self):
        offset = 0
        increment = 20
        for post_type in self.post_types:
            results = []
            while True:
                post_filter = {'post_type': post_type, 'number': increment, 'offset': offset}
                result = self.client.call(posts.GetPosts(post_filter))
                if len(result) == 0:
                    break  # no more posts returned
                for post in result:
                    results.append(post)
                offset = offset + increment
            self.posts.update({post_type: results})

    def new_post(self, title, content, tags, attachment_id):
        post = WordPressPost()
        post.title = title
        post.content = content
        post.terms = tags
        post.thumbnail = attachment_id
        post.post_status = 'publish'
        post.id = self.client.call(posts.NewPost(post))

    def new_term(self, taxonomy, name, parent_id):
        term = WordPressTerm()

        term.taxonomy = taxonomy
        term.name = name
        term.parent = parent_id

        term.id = self.client.call(taxonomies.NewTerm(term))

    def find_term(self, name, taxonomy=None):
        taxonomy_list = []
        if not taxonomy:
            for t in self.taxonomies:
                taxonomy_list.extend(self.taxonomies[t])
        else:
            taxonomy_list.extend(self.taxonomies[taxonomy])

        for term in taxonomy_list:
            if term.name == name:
                return term

    def upload_image(self, file_path):
        filename = basename(file_path)
        data = {'name': filename, 'type': 'image/jpeg' if splitext(filename)[1] == ".jpg" else 'image/png'}

        with open(file_path, 'rb') as img:
            data['bits'] = xmlrpc_client.Binary(img.read())

        response = self.client.call(media.UploadFile(data))
        return response

    def update_posts(self, post_type=None, mode="wp"):
        """
        Updates posts, if post_type is None all posts are updated.
        :param post_type: default is None , post type filter
        :param mode: default is "wp" updates the Wordpress post, "db" updates the db entry, "hana" updates both.
        """
        if not len(self.posts):
            self.index_posts()

        post_list = []

        if not post_type:
            for p in self.posts:
                post_list.extend(self.posts[p])
        else:
            post_list.extend(self.posts[post_type])

        for post in post_list:
            if mode == "wp":
                self.update_post_wp(post.id)
            elif mode == "db":
                self.update_post_db(post.id)
            elif mode == "hana":
                self.update_post_wp(post.id)
                self.update_post_db(post.id)

    def update_post_wp(self, post_id):
        """
        Update post with id in WordPress.
        :param post_id: post id
        """
        post = self.client.call(posts.GetPost(post_id))
        if not post:
            raise ResourceError("Post not found, id: {}.".format(post_id))

        lat, long = None, None
        wp_geo_hash_id = None
        location_serialized = None
        for field in post.custom_fields:
            if field["key"] == "city":
                location_serialized = field
            if field["key"] == "geo_hash_id":
                wp_geo_hash_id = field
            if field["key"] == "coordinates_lat":
                lat = float(field["value"])
            if field["key"] == "coordinates_long":
                long = float(field["value"])
        db_post = self.connection.get(InterestPoint, {"latitude": lat, "longitude": long})
        if not db_post:
            db_post = self.connection.get(InterestPoint, {"title": post.title})
        if not db_post:
            raise MissingResourceError("InterestPoint not found for post with id: {}.".format(post.id))

        new_post_data = WordPressPost()

        if wp_geo_hash_id["value"] != db_post.geo_hash_id:
            wp_geo_hash_id["value"] = db_post.geo_hash_id
            new_post_data.custom_fields = [wp_geo_hash_id]

        location = []
        location_unserialized = loads(bytes(location_serialized["value"], 'utf-8'))
        for loc in location_unserialized:
            for term in self.taxonomies["locations"]:
                if term.id == str(location_unserialized[loc]):
                    location.append(term)
                    break

        if db_post.city.name not in location:
            city_term = self.find_term(db_post.city.name, "locations")
            location[2] = city_term
            location_serialized["value"] = dumps([x.id for x in location])
            new_post_data.custom_fields.append(location_serialized)

        self.client.call(posts.EditPost(post.id, new_post_data))
        exit()

    def update_post_db(self, post_id):
        """
        Update post with id in database.
        :param post_id: post id
        """
        pass
