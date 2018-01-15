"""
    Module used for database connection. All operations on the database should be handled in here
"""
from sqlalchemy import MetaData, engine_from_config, not_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import scoped_session, sessionmaker, load_only


class DataBaseView:
    """Db connection"""
    def __init__(self, access, echo=False):
        conf = access.connection_string
        config = {"sqlalchemy.url": "mysql+mysqlconnector://" + conf["dbuser"] + ":" + conf["dbpass"] + "@" +
                                    conf["dbhost"] + ":" + conf["dbport"] + "/" + conf["dbname"] + "?charset=utf8",
                  "sqlalchemy.echo": str(echo)}
        metadata = MetaData()
        engine = engine_from_config(config)
        session = scoped_session(sessionmaker(autoflush=False, autocommit=False, bind=engine))
        self.session = session()
        metadata.create_all(bind=engine)

    # multiple select
    def index(self, cls, columns=None, match_filter=None, filter_mode="match", limit=None):
        """Get all items."""
        items = self.session.query(cls)
        if columns:
            items = items.options(load_only(*columns))
        if match_filter:
            if filter_mode == "like":
                for attr, value in match_filter.items():
                    if isinstance(value, str):
                        items = items.filter(getattr(cls, attr).like("%{}%".format(value)))
                    else:
                        items = items.filter(getattr(cls, attr) == value)
            elif filter_mode == "exclude":
                for attr, value in match_filter.items():
                    if isinstance(value, str):
                        items = items.filter(not_(getattr(cls, attr).like("%{}%".format(value))))
                    else:
                        items = items.filter(getattr(cls, attr).isnot(value))
            else:
                items = items.filter_by(**match_filter)
        if limit:
            items = items.limit(int(limit))
        return items.all()

    # single select
    def get(self, cls, match_filter=None, filter_mode="match"):
        """Get an item. select equivalent"""
        item = self.session.query(cls)
        if match_filter:
            if filter_mode == "like":
                for attr, value in match_filter.items():
                    if isinstance(value, str):
                        item = item.filter(getattr(cls, attr).like("%{}%".format(value)))
                    else:
                        item = item.filter(getattr(cls, attr) == value)
            elif filter_mode == "exclude":
                for attr, value in match_filter.items():
                    if isinstance(value, str):
                        item = item.filter(not_(getattr(cls, attr).like("%{}%".format(value))))
                    else:
                        item = item.filter(getattr(cls, attr).isnot(value))
            else:
                item = item.filter_by(**match_filter)
        try:
            item = item.first()
        except Exception as e:
            print("Connection unavailable : {}".format(e))
            raise
        #     item = item.scalar()
        # except MultipleResultsFound:
        return item

    # insert
    def post(self, data):
        """Insert a new item. insert equivalent"""
        self.session.merge(data)
        try:
            self.session.commit()
        except IntegrityError as e:
            self.session.rollback()
            if e.orig.errno != 1062:
                print(e)
                raise

    # delete
    def delete(self, cls, match_id):
        """Delete an item. delete equivalent"""
        try:
            self.session.query(cls).filter(cls.id == match_id).delete()
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

    # update
    def put(self, cls, match_id, data):
        """Update an item. update equivalent"""
        try:
            self.session.query(cls).filter(cls.id == match_id).update(data, synchronize_session='fetch')
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
