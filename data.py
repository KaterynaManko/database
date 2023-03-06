from sqlalchemy import Table, Column, Integer, String, Float, MetaData
from sqlalchemy import create_engine
import csv

engine = create_engine('sqlite:///database.db')

meta = MetaData()

stations = Table(
   'stations', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('latitude', Float),
   Column('longitude', Float),
   Column('elevation', Float),
   Column('name', String),
   Column('country', String),
   Column('state', String),
)

stations_measure = Table(
   'stations_measure', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('date', String),
   Column('precip', Float),
   Column('tobs', Integer),
)

meta.create_all(engine)

conn = engine.connect()
with open('clean_stations.csv', newline='') as csvfile:
 reader = csv.DictReader(csvfile)
 for row in reader:
  conn.execute(stations.insert(), [
      {'station': row['station'], 'latitude': row['latitude'], 'longitude': row['longitude'], 'elevation': row['elevation'], 
      'name': row['name'], 'country': row['country'], 'state': row['state']},
      ])

conn = engine.connect()
with open('clean_measure.csv', newline='') as csvfile:
 reader = csv.DictReader(csvfile)
 for row in reader:
  conn.execute(stations_measure.insert(), [
      {'station': row['station'], 'date': row['date'], 'precip': row['precip'], 'tobs': row['tobs']},
      ]) 
  
conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
