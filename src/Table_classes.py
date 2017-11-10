from sqlalchemy import Column, String, Integer, Date
from sqlalchemy.ext.declarative import declarative_base

# Table base class
Declarative_Base = declarative_base()


# ------------------------------------------------------
# Table classes used for creating tables (if not exist)
# ------------------------------------------------------
class Airport(Declarative_Base):
    __tablename__ = 'airport'

    # schema
    id = Column(Integer, primary_key=True)
    name = Column(String(150))
    city = Column(String(200))
    state = Column(String(3))
    country = Column(String(40))
    location = Column(String(250))

    def __init__(self, id, name, city, state, country, location):
        self.id = id
        self.name = name
        self.city = city
        self.state = state
        self.country = country
        self.location = location


class Carrier_History(Declarative_Base):
    __tablename__ = 'carrier_history'

    # schema
    unique_code = Column(String(25), primary_key=True)
    code = Column(String(5))
    description = Column(String(250))
    start_year = Column(Integer)
    end_year = Column(Integer)

    def __init__(self, unique_code, code, description, start_year, end_year=None):
        self.unique_code = unique_code
        self.code = code
        self.description = description
        self.start_year = start_year
        self.end_year = end_year


class System_Fields(Declarative_Base):
    __tablename__ = 'system_fields'

    # schema
    name = Column(String(30), primary_key=True)
    description = Column(String)

    def __init__(self, name, description):
        self.name = name
        self.description = description
