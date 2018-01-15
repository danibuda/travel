from wordpress_xmlrpc import Client, WordPressPost, WordPressTerm
from wordpress_xmlrpc.methods import taxonomies, posts, media
from wordpress_xmlrpc.compat import xmlrpc_client
from scripts.exceptions import ResourceError
from os.path import basename, splitext


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

    def upload_image(self, file_path):
        filename = basename(file_path)
        data = {'name': filename, 'type': 'image/jpeg' if splitext(filename)[1] == ".jpg" else 'image/png'}

        with open(file_path, 'rb') as img:
            data['bits'] = xmlrpc_client.Binary(img.read())

        response = self.client.call(media.UploadFile(data))
        return response

    def update_posts(self, taxonomy=None):
        pass
