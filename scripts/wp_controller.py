from wordpress_xmlrpc import Client, WordPressPost, WordPressTerm
from wordpress_xmlrpc.methods import taxonomies, posts, media
from wordpress_xmlrpc.compat import xmlrpc_client
from database.mappings import *
from scripts.exceptions import *
from os.path import basename, splitext
from phpserialize import *


class CustomFields:

    def __init__(self, field_list, key_association=None):
        self.list = [{"key": field["key"], "value": field["value"]} for field in field_list]
        self.key_association = key_association

    def pack(self):
        result = []
        invert_key_assoc = {v: k for k, v in self.key_association.items()}
        for item in self.list:
            if invert_key_assoc:
                unpacked_field_key = invert_key_assoc[item["key"]]
            else:
                unpacked_field_key = item["key"]

            field_value = item["value"]

            if isinstance(field_value, list):
                unpacked_field_value = dumps([x.id for x in field_value])
            else:
                unpacked_field_value = str(field_value)

            result.append({"key": unpacked_field_key, "value": unpacked_field_value})

        return result

    def unpack(self):
        result = {}
        try:
            for item in self.list:
                try:
                    unpacked_field_key = self.key_association[item["key"]]
                except KeyError:
                    continue

                field_value = item["value"]

                if field_value.count(";i:") > 1:
                    unpacked_field_value = loads(bytes(field_value))
                elif field_value.count(".") == 1 and field_value.replace(".", "").isalnum():
                    unpacked_field_value = float(field_value)
                else:
                    unpacked_field_value = field_value

                result.update({unpacked_field_key: unpacked_field_value})
        except AttributeError as e:
            print(e)

        return result


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

    def get_posts(self, post_type=None):
        results = []
        if not post_type:
            return results
        offset = 0
        increment = 20
        while True:
            post_filter = {'post_type': post_type, 'number': increment, 'offset': offset}
            result = self.client.call(posts.GetPosts(post_filter))
            if len(result) == 0:
                break  # no more posts returned
            results.extend(result)
            offset = offset + increment
        return results

    def index_posts(self, post_type=None):
        if post_type:
            self.posts.update({post_type: self.get_posts(post_type)})
        for some_type in self.post_types:
            self.posts.update({some_type: self.get_posts(some_type)})

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
        :param mode: default is "wp" updates the Wordpress post,
                     "db" updates the db entry,
                     "hana" updates both.
        """
        if not len(self.posts) and post_type:
            self.index_posts(post_type)
        else:
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

    def update_post_wp(self, wp_post_id):
        """
        Update post with id in WordPress.
        :param wp_post_id: post id
        """
        post = self.client.call(posts.GetPost(wp_post_id))

        if not post:
            raise ResourceError("Post not found, id: {}.".format(wp_post_id))

        key_assoc = {"city": "city_id", "coordinates_lat": "latitude", "coordinates_long": "longitude",
                     "geo_hash_id": "geo_hash_id", "rate": "rate", "place_id": "place_id",
                     "address": "address_coord"}
        post_custom_fields = CustomFields(post.custom_fields, key_assoc)
        custom_fields_unpacked = post_custom_fields.unpack()

        try:
            location = []
            location_unserialized = custom_fields_unpacked["city_id"]
            for loc in location_unserialized:
                term = self.client.call(taxonomies.GetTerm("locations", int(loc)))
                location.append(term)
                break
        except KeyError:
            pass

        db_post = self.connection.get(InterestPoint, {"latitude": custom_fields_unpacked["latitude"],
                                                      "longitude": custom_fields_unpacked["longitude"]})
        if not db_post:
            db_post = self.connection.get(InterestPoint, {"title": post.title})

        if not db_post:
            raise MissingResourceError("InterestPoint not found for post with id: {}.".format(post.id))

        dict_post = post.__dict__()

        post_diff = self.prejudice_diff(post, db_post)

        new_post_data = WordPressPost()
        new_post_data.custom_fields = []

        self.client.call(posts.EditPost(post.id, new_post_data))
        exit()

    def update_post_db(self, db_post_id):
        """
        Update post with id in database.
        :param db_post_id: post id
        """
        pass

    def prejudice_diff(self, wp_post, db_post):
        pass
