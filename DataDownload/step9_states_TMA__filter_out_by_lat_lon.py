from config import *
import warnings
warnings.filterwarnings('ignore')

import os

DATA_DIR = os.path.join("..", "Data")
DATA_DIR = os.path.join(DATA_DIR, AIRPORT_ICAO)
DATA_DIR = os.path.join(DATA_DIR, YEAR)

area = (str(RADIUS) + "NM", "TMA")[AREA == "TMA"]
INPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + '_' + YEAR + "_filtered_by_altitude")
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + '_' + YEAR + "_filtered_by_latitude")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

import pandas as pd
import numpy as np
import calendar

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

from geopy.distance import geodesic

import time
start_time = time.time()

flight_type = "Departure" if DEPARTURE else "Arrival"

def circle_contains(radius, point):
    central_point = (CENTRAL_LAT, CENTRAL_LON)
    distance = geodesic(central_point, point).meters
    
    if distance < radius:
        return True
    else:
        return False

for month in MONTHS:
    
    for week in WEEKS:
        
        if week == 5 and month == '02' and not calendar.isleap(int(YEAR)):
            continue
        
        filename = AIRPORT_ICAO + '_states_'+  area + '_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
        
        if DEPARTURE:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_arrival_' + filename
        
        full_filename = os.path.join(INPUT_DIR, filename)
        
        
        df = pd.read_csv(full_filename, sep=' ',
            names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity',  'beginDate', 'endDate'],
            dtype={'sequence':int, 'timestamp':int, 'rawAltitude':int, 'altitude':int, 'beginDate':str, 'endDate':str})

        df.set_index(['flightId', 'sequence'], inplace = True)

        number_of_flights = len(df.groupby(level='flightId'))
        count = 0

        for flight_id, flight_id_group in df.groupby(level='flightId'):
            
            count = count + 1
            print("STEP9", flight_type, area, AIRPORT_ICAO, YEAR, month, week, number_of_flights, count, flight_id)
            
            ###################################################################
            # Latitude or longitude outside of TMA too much
            ###################################################################
            
            lon_min = min(TMA_lon) - 0.5
            lon_max = max(TMA_lon) + 0.5
            lat_min = min(TMA_lat) - 0.5
            lat_max = max(TMA_lat) + 0.5
            
            rect_lon = [lon_min, lon_min, lon_max, lon_max, lon_min]
            rect_lat = [lat_min, lat_max, lat_max, lat_min, lat_min]
            
            lons_lats_vect = np.column_stack((rect_lon, rect_lat)) # Reshape coordinates
            polygon = Polygon(lons_lats_vect) # create polygon
            
            flight_states_df = flight_id_group.copy() 
            
            flight_states_df.reset_index(drop = False, inplace = True)
            df_len = len(flight_states_df)
            flight_states_df.set_index('sequence', inplace=True)
            
            remove = False
            if not flight_states_df.empty:
                
                for seq, row in flight_states_df.iterrows():
                    
                    if AREA == "TMA":
                        
                        point = Point(row["lon"], row["lat"])
                        
                        if not polygon.contains(point):
                            remove = True
                            break
                    
                    else: # AREA == "CIRCLE"
                        
                        lat = row['lat']
                        lon = row['lon']
                        point = (lat, lon)
                        
                        radius = (RADIUS+30)*1852 # 30NM ~ 0.5 degree
                        
                        if not circle_contains(radius, point):
                            remove = True
                            break
                
                if remove:
                    df = df.drop(flight_id)
                    continue
        
        
        filename = AIRPORT_ICAO + '_states_'+ area +'_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
        
        if DEPARTURE:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_arrival_' + filename

        full_filename = os.path.join(OUTPUT_DIR, filename)
        
        df.to_csv(full_filename, sep=' ', encoding='utf-8', float_format='%.6f', header=False, index=True)

print((time.time()-start_time)/60)
