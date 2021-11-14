from config import *

import os

DATA_DIR = os.path.join("..", "Data")
DATA_DIR = os.path.join(DATA_DIR, AIRPORT_ICAO)
DATA_DIR = os.path.join(DATA_DIR, YEAR)

area_input = ("around" + str(RADIUS) + "NM", "aroundTMA")[AREA == "TMA"]
INPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area_input + "_" + YEAR + "_fixed_lat_lon")
area_output = (str(RADIUS) + "NM", "TMA")[AREA == "TMA"]
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area_output + '_' + YEAR + "_extracted")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

import pandas as pd
import numpy as np
import calendar

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

from geopy.distance import geodesic

if AIRPORT_ICAO == "ESSA":
    from constants_ESSA import *
elif AIRPORT_ICAO == "ESGG":
    from constants_ESGG import *
elif AIRPORT_ICAO == "EIDW":
    from constants_EIDW import *
elif AIRPORT_ICAO == "LOWW":
    from constants_LOWW import *
    
flight_type = "Departure" if DEPARTURE else "Arrival"


def get_states_inside_TMA(states_df, month, week):
    
    filename = AIRPORT_ICAO + '_states_' + area_output + '_extracted_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
    if DEPARTURE:
        filename = 'osn_departure_' + filename
    else:
        filename = 'osn_arrival_' + filename
        
    full_output_filename = os.path.join(OUTPUT_DIR, filename)

    states_inside_TMA_df = pd.DataFrame()

    number_of_flights = len(states_df.groupby(level='flightId'))
    count = 0
    
    for flight_id, flight_df in states_df.groupby(level='flightId'):
        
        count = count + 1
        
        print("STEP5", flight_type, area_output, AIRPORT_ICAO, YEAR, month, week, number_of_flights, count)
        
        if DEPARTURE:
            last_point_index = get_TMA_last_point_index(flight_id, flight_df)
            
            if last_point_index==-1:
                continue
            
            if last_point_index==0:
                continue
            
            #print(last_point_index)
            
            new_df_inside_TMA = flight_df.loc[flight_df.index.get_level_values('sequence') < last_point_index]
            
        else: # arrival
            first_point_index = get_TMA_first_point_index(flight_id, flight_df)
            
            if first_point_index==-1:
                continue
            
            new_df_inside_TMA = flight_df.loc[flight_df.index.get_level_values('sequence') >= first_point_index]
            
            new_df_inside_TMA.reset_index(drop=False, inplace=True)
            
            # reassign sequence
            new_df_inside_TMA_length = len(new_df_inside_TMA)
            
            sequence_list = list(range(new_df_inside_TMA_length))
            
            new_df_inside_TMA = new_df_inside_TMA.sort_values(by=['timestamp'])
            
            new_df_inside_TMA.drop(['sequence'], axis=1, inplace=True)
            
            new_df_inside_TMA['sequence'] = sequence_list
            new_df_inside_TMA.set_index(['flightId', 'sequence'], drop=True, inplace=True)
            
        states_inside_TMA_df = states_inside_TMA_df.append(new_df_inside_TMA)
    
    states_inside_TMA_df.to_csv(full_output_filename, sep=' ', encoding='utf-8', float_format='%.6f', header=None, index=True)


def get_TMA_last_point_index(flight_id, flight_df):
    
    lat = 0
    lon = 0
    for seq, row in flight_df.groupby(level='sequence'):
        lat = row.loc[(flight_id, seq)]['lat']
        lon = row.loc[(flight_id, seq)]['lon']
        if (not check_TMA_contains_point(Point(lon, lat))):
            #print(seq)
            #print(lon, lat)
            return seq
    
    print("-1", lat, lon)
    return -1
 
def get_TMA_first_point_index(flight_id, flight_df):
    
    lat = 0
    lon = 0
    for seq, row in flight_df.groupby(level='sequence'):
        lat = row.loc[(flight_id, seq)]['lat']
        lon = row.loc[(flight_id, seq)]['lon']
        if (check_TMA_contains_point(Point(lon, lat))):
            #print(seq)
            #print(lon, lat)
            return seq
    
    print("-1", lat, lon)
    return -1
 
def check_TMA_contains_point(point):

    lons_lats_vect = np.column_stack((TMA_lon, TMA_lat)) # Reshape coordinates
    polygon = Polygon(lons_lats_vect) # create polygon

    return polygon.contains(point)


def get_states_inside_circle(states_df, month, week):
    
    filename = AIRPORT_ICAO + '_states_' + area_output + '_extracted_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
    if DEPARTURE:
        filename = 'osn_departure_' + filename
    else:
        filename = 'osn_arrival_' + filename
     
    full_output_filename = os.path.join(OUTPUT_DIR, filename)
    
    states_inside_circle_df = pd.DataFrame()
    
    number_of_flights = len(states_df.groupby(level='flightId'))
    count = 0
    
    for flight_id, flight_df in states_df.groupby(level='flightId'):
        
        count = count + 1
        
        print("STEP5", flight_type, area_output, AIRPORT_ICAO, YEAR, month, week, number_of_flights, count)
        
        if DEPARTURE:
            last_point_index = get_circle_last_point_index(flight_id, flight_df)
            
            if last_point_index==-1:
                continue
            
            new_df_inside_circle = flight_df.loc[flight_df.index.get_level_values('sequence') <= last_point_index]
            
        else: # arrival
            first_point_index = get_circle_first_point_index(flight_id, flight_df)
            
            if first_point_index==-1:
                continue
            
            new_df_inside_circle = flight_df.loc[flight_df.index.get_level_values('sequence') >= first_point_index]
            
            new_df_inside_circle.reset_index(drop=False, inplace=True)
            
            # reassign sequence
            new_df_inside_circle_length = len(new_df_inside_circle)
            
            sequence_list = list(range(new_df_inside_circle_length))
            
            new_df_inside_circle = new_df_inside_circle.sort_values(by=['timestamp'])
            
            new_df_inside_circle.drop(['sequence'], axis=1, inplace=True)
            
            new_df_inside_circle['sequence'] = sequence_list
            
            new_df_inside_circle.set_index(['flightId', 'sequence'], drop=True, inplace=True)
            
            
        states_inside_circle_df = states_inside_circle_df.append(new_df_inside_circle)
    
    
    number_of_flights = len(states_inside_circle_df.groupby(level='flightId'))
    
    states_inside_circle_df.to_csv(full_output_filename, sep=' ', encoding='utf-8', float_format='%.6f', header=None, index=True)


def get_circle_last_point_index(flight_id, flight_df):
    
    lat = 0
    lon = 0
    for seq, row in flight_df.groupby(level='sequence'):
        lat = row.loc[(flight_id, seq)]['lat']
        lon = row.loc[(flight_id, seq)]['lon']
        if not check_circle_contains_point((lat, lon)):
            #print(seq)
            #print(lon, lat)
            return seq-1
    
    print("-1", lat, lon)
    return -1

def get_circle_first_point_index(flight_id, flight_df):
    
    lat = 0
    lon = 0
    for seq, row in flight_df.groupby(level='sequence'):
        lat = row.loc[(flight_id, seq)]['lat']
        lon = row.loc[(flight_id, seq)]['lon']
        if check_circle_contains_point((lat, lon)):
            #print(seq)
            #print(lon, lat)
            return seq
    
    print("-1", lat, lon)
    return -1


def check_circle_contains_point(point):
    central_point = (CENTRAL_LAT, CENTRAL_LON)
    distance = geodesic(central_point, point).meters
    
    if distance < RADIUS*1852:
        return True
    else:
        return False


import time
start_time = time.time()


for month in MONTHS:
    
    for week in WEEKS:
        
        if week == 5 and month == '02' and not calendar.isleap(int(YEAR)):
            continue
        
        filename = AIRPORT_ICAO + '_states_' + area_input + '_fixed_lat_lon_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
        
        if DEPARTURE:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_arrival_' + filename
        
        full_input_filename = os.path.join(INPUT_DIR, filename)
        
        df = pd.read_csv(full_input_filename, sep=' ',
            names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'altitude', 'velocity', 'beginDate', 'endDate'],
            index_col=[0,1],
            dtype={'flightId':str, 'sequence':int, 'timestamp':str, 'lat':float, 'lon':float, 'altitude':int, 'velocity':int, 'beginDate':str, 'endDate':str})
        
        if AREA=="TMA":
            get_states_inside_TMA(df, month, week)
        else:
            get_states_inside_circle(df, month, week)

print((time.time()-start_time)/60)

