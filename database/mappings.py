from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Float, DateTime
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.orm import relationship

Base = declarative_base()


class BookingListing(Base):
    __tablename__ = 'booking_listings'

    id = Column(Integer, primary_key=True)
    geo_hash_id = Column(String(64))
    title = Column(String(250), nullable=True)
    county_id = Column(Integer, ForeignKey('account_county.id'), nullable=True)
    type = Column(String(50), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    city_id = Column(Integer, ForeignKey('account_city.id'), nullable=True)
    hotel_id = Column(Integer, nullable=True)
    telephone = Column(String(12), nullable=True)
    address = Column(String(250), nullable=True)
    place_id = Column(String(50), nullable=True)
    photos = Column(String(250), nullable=True)
    status = Column(String(50), nullable=True)
    contents = relationship('BookingListingContent', backref='booking_listings_content')
    facilities = relationship('BookingListingFacility', backref='booking_listings_facility')
    platforms = relationship('BookingListingPlatform', backref='booking_listings_platform')

    def __init__(self, *args, **kwargs):
        super(BookingListing, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '<Listing id: {}, title: {}, city_id: {}>'.format(self.id, self.title, self.city_id)

    def update(self, data):
        self.__dict__.update(data)


class BookingListingContent(Base):
    __tablename__ = 'booking_listings_content'

    id = Column(Integer, primary_key=True)
    geo_hash_id = Column(String(64), ForeignKey('booking_listings.geo_hash_id'))
    type = Column(String(50))
    src_url = Column(String(255))
    content = Column(MEDIUMTEXT)

    def __init__(self, *args, **kwargs):
        super(BookingListingContent, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '<Content id: {}, type: {}>'.format(self.id, self.type)


class BookingListingFacility(Base):
    __tablename__ = 'booking_listings_facility'

    id = Column(Integer, primary_key=True)
    geo_hash_id = Column(String(64), ForeignKey('booking_listings.geo_hash_id'))
    type = Column(String(50))
    content = Column(MEDIUMTEXT)

    def __init__(self, *args, **kwargs):
        super(BookingListingFacility, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '<Facility id: {}, type: {}>'.format(self.id, self.type)


class BookingListingPlatform(Base):
    __tablename__ = 'booking_listings_platform'

    id = Column(Integer, primary_key=True)
    geo_hash_id = Column(String(64), ForeignKey('booking_listings.geo_hash_id'))
    platform = Column(String(50))
    url = Column(String(255))
    avg_price = Column(String(10), nullable=True)
    last_modified = Column(DateTime)

    def __init__(self, *args, **kwargs):
        super(BookingListingPlatform, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '<Platform id: {}, name: {}>'.format(self.id, self.platform)


class City(Base):
    __tablename__ = 'account_city'

    id = Column(Integer, primary_key=True)
    county_id = Column(Integer, ForeignKey('account_county.id'))
    siruta = Column(Integer)
    longitude = Column(Float)
    latitude = Column(Float)
    name = Column(String(64))
    region = Column(String(64))
    check = Column(String(5), nullable=True)
    listings = relationship('BookingListing', backref='listing_city')
    interest_points = relationship('InterestPoint', backref='city')

    def __repr__(self):
        return '<City: id: {} name: {}>'.format(self.id, self.name)


class County(Base):
    __tablename__ = 'account_county'

    id = Column(Integer, primary_key=True)
    code = Column(String(2))
    name = Column(String(64))
    cities = relationship('City', backref='county')
    listings = relationship('BookingListing', backref='listing_county')

    def __repr__(self):
        return '<County: id: {}, name: {}, code: {}>'.format(self.id, self.name, self.code)


class Church(Base):
    __tablename__ = 'biserici_romania'

    id = Column(Integer, primary_key=True)
    geo_hash_id = Column(String(64), nullable=True)
    title = Column(String(255))
    religion = Column(String(50), nullable=True)
    city_id = Column(Integer, ForeignKey('account_city.id'), nullable=True)
    county_id = Column(Integer, ForeignKey('account_county.id'), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    cod_lmi = Column(String(50), nullable=True)
    photo_cnt = Column(Integer, nullable=True)
    url = Column(String(255), nullable=True)
    address = Column(String(255), nullable=True)
    telephone = Column(String(15), nullable=True)

    def __init__(self, *args, **kwargs):
        super(Church, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '<Church id: {}, title: {}, city_id: {}>'.format(self.id, self.title, self.city_id)

    def update(self, data):
        self.__dict__.update(data)


class InterestPoint(Base):
    __tablename__ = 'interest_points'

    id = Column(Integer, primary_key=True)
    geo_hash_id = Column(String(64))
    title = Column(String(250), nullable=True)
    city_id = Column(Integer, ForeignKey('account_city.id'), nullable=True)
    types = Column(String(255), nullable=True)
    wp_post_id = Column(String(50), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    address_coord = Column(String(250), nullable=True)
    rate = Column(Float, nullable=True)
    place_id = Column(String(50), nullable=True)
    photos = Column(String(250), nullable=True)
    status = Column(String(50), nullable=True)
    check = Column(String(50), nullable=True)

    contents = relationship('InterestPointContent', backref='interest_points_content')
    facilities = relationship('InterestPointFacility', backref='interest_points_facility')
    platforms = relationship('InterestPointPlatform', backref='interest_points_platform')

    def __init__(self, *args, **kwargs):
        super(InterestPoint, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '<InterestPoint id: {}, title: {}, city_id: {}>'.format(self.id, self.title, self.city_id)

    def update(self, **kwargs):
        self.__dict__.update(kwargs)


class InterestPointPlatform(Base):
    __tablename__ = 'interest_points_platform'

    id = Column(Integer, primary_key=True)
    geo_hash_id = Column(String(64), ForeignKey('interest_points.geo_hash_id'))
    platform = Column(String(50))
    url = Column(String(255))
    avg_price = Column(String(10), nullable=True)
    last_modified = Column(DateTime)

    def __init__(self, *args, **kwargs):
        super(InterestPointPlatform, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '<Platform id: {}, name: {}>'.format(self.id, self.platform)


class InterestPointContent(Base):
    __tablename__ = 'interest_points_content'

    id = Column(Integer, primary_key=True)
    geo_hash_id = Column(String(64), ForeignKey('interest_points.geo_hash_id'))
    type = Column(String(50))
    src_url = Column(String(255))
    content = Column(MEDIUMTEXT)

    def __init__(self, *args, **kwargs):
        super(InterestPointContent, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '<Content id: {}, type: {}>'.format(self.id, self.type)


class InterestPointFacility(Base):
    __tablename__ = 'interest_points_facility'

    id = Column(Integer, primary_key=True)
    geo_hash_id = Column(String(64), ForeignKey('interest_points.geo_hash_id'))
    type = Column(String(50))
    content = Column(MEDIUMTEXT)

    def __init__(self, *args, **kwargs):
        super(InterestPointFacility, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '<Facility id: {}, type: {}>'.format(self.id, self.type)
