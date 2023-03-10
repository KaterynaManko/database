from sqlalchemy import Column, Integer, String, Float
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
import csv

Base = declarative_base()
engine = create_engine('sqlite:///station.db')
session = Session(bind=engine)

class Station(Base):
 __tablename__ = "stations"
 id = Column(Integer, primary_key=True)
 station = Column(String)
 latitude = Column(Float)
 longitude = Column(Float)
 elevation = Column(Float)
 name = Column(String)
 country = Column(String)
 state = Column(String)
 
class Measure(Base):
   __tablename__ = "stations_measure"
   id = Column(Integer, primary_key=True)
   station = Column(String)
   date = Column(String)
   precip = Column(Float)
   tobs = Column(Integer)

Base.metadata.create_all(engine)

def add_stations():
 with open('clean_stations.csv', newline='') as csvfile:
  reader = csv.DictReader(csvfile)
  stations = [{'station': row['station'], 'latitude': row['latitude'], 'longitude': row['longitude'], 'elevation': row['elevation'], 
      'name': row['name'], 'country': row['country'], 'state': row['state']} for row in reader]
  session.bulk_insert_mappings(Station, stations)
  session.commit()

def add_stations_measure():
 with open('clean_measure.csv', newline='') as csvfile:
  reader = csv.DictReader(csvfile)
  measure = [{'station': row['station'], 'date': row['date'], 'precip': row['precip'], 'tobs': row['tobs']} for row in reader] 
  session.bulk_insert_mappings(Measure, measure)
  session.commit()

  
add_stations()   
add_stations_measure()
session.execute("SELECT * FROM stations LIMIT 5").fetchall()