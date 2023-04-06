from config import *
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import calendar

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

from geopy.distance import geodesic

import os

DATA_DIR = os.path.join("..", "Data")
DATA_DIR = os.path.join(DATA_DIR, AIRPORT_ICAO)
DATA_DIR = os.path.join(DATA_DIR, YEAR)

INPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_tracks_" + YEAR)
area = "around" + str(RADIUS) + "NM" if AREA == "CIRCLE" else "aroundTMA"
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_tracks_" + area + "_" + YEAR)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

flight_type = "Departure" if DEPARTURE else "Arrival"


def get_all_tracks(csv_input_file):

    if DEPARTURE:
        df = pd.read_csv(os.path.join(INPUT_DIR, csv_input_file), sep=' ',
            names = ['flightId', 'sequence', 'destination', 'beginDate', 'callsign', 'icao24', 'timestamp', 'lat', 'lon', 'baroAltitude'],
            index_col=[0,1],
            dtype={'flightId':str, 'sequence':int, 'beginDate':str})
    else:
        df = pd.read_csv(os.path.join(INPUT_DIR, csv_input_file), sep=' ',
            names = ['flightId', 'sequence', 'origin', 'endDate', 'callsign', 'icao24', 'timestamp', 'lat', 'lon', 'baroAltitude'],
            index_col=[0,1],
            dtype={'flightId':str, 'sequence':int, 'endDate':str})

    return df


# departures - start from the first waypoint outside TMA
# arrivals - start from the last waypont outside TMA
def get_tracks_inside_TMA(month, week, tracks_df, csv_output_file):

    tracks_inside_TMA_df = pd.DataFrame()

    number_of_flights = len(tracks_df.groupby(level='flightId'))

    count = 0
    for flight_id, new_df in tracks_df.groupby(level='flightId'):
        
        count = count + 1
        
        print("STEP2", flight_type, area, AIRPORT_ICAO, YEAR, month, week, number_of_flights, count, flight_id)
        
        new_df_inside_TMA = pd.DataFrame()
        
        if DEPARTURE:
            first_point_outside_TMA_index = get_first_point_outside_TMA(flight_id, new_df)
            
            if first_point_outside_TMA_index == -1 or first_point_outside_TMA_index == 0:
                continue
            
            new_df_inside_TMA = new_df.iloc[:first_point_outside_TMA_index+1].copy()
        
        else:
            first_point_inside_TMA_index = get_first_point_inside_TMA(flight_id, new_df)
            
            if first_point_inside_TMA_index == -1 or first_point_inside_TMA_index == 0:
                continue
            
            last_point_outside_TMA_index = first_point_inside_TMA_index - 1
            
            new_df_inside_TMA = new_df.iloc[last_point_outside_TMA_index:].copy()
            
            # reassign sequence
            new_df_inside_TMA.reset_index(drop=False, inplace=True)
            new_df_inside_TMA_length = len(new_df_inside_TMA)
            
            sequence_list = list(range(new_df_inside_TMA_length))
            
            new_df_inside_TMA.drop(['sequence'], axis=1, inplace=True)
            
            new_df_inside_TMA['sequence'] = sequence_list
            new_df_inside_TMA.set_index(['flightId', 'sequence'], drop=True, inplace=True)
            
            
        tracks_inside_TMA_df = tracks_inside_TMA_df.append(new_df_inside_TMA)
        
    tracks_inside_TMA_df.to_csv(os.path.join(OUTPUT_DIR, csv_output_file), sep=' ', encoding='utf-8', float_format='%.6f', header=None, index=True)


# for arrivals
def get_first_point_inside_TMA(flight_id, new_df):
    
    lat = 0.0
    lon = 0.0
    for seq, row in new_df.groupby(level='sequence'):
        lat = row.loc[(flight_id, seq)]['lat']
        lon = row.loc[(flight_id, seq)]['lon']
        if check_TMA_contains_point(Point(lon, lat)):
            return seq
    print(lat, lon)
    return -1

# for departures
def get_first_point_outside_TMA(flight_id, new_df):
    
    lat = 0.0
    lon = 0.0
    for seq, row in new_df.groupby(level='sequence'):
        lat = row.loc[(flight_id, seq)]['lat']
        lon = row.loc[(flight_id, seq)]['lon']
        if not check_TMA_contains_point(Point(lon, lat)):
            return seq
    print(lat, lon)
    return -1


def check_TMA_contains_point(point):

    lons_lats_vect = np.column_stack((TMA_lon, TMA_lat)) # Reshape coordinates
    polygon = Polygon(lons_lats_vect) # create polygon

    return polygon.contains(point)


# departures - end with the first waypoint outside the circle
# arrivals - start from the last waypont outside the circle
def get_tracks_inside_circle(month, week, tracks_df, csv_output_file):

    tracks_inside_circle_df = pd.DataFrame()

    number_of_flights = len(tracks_df.groupby(level='flightId'))

    count = 1
    for flight_id, new_df in tracks_df.groupby(level='flightId'):
        
        print("STEP2", flight_type, area, AIRPORT_ICAO, YEAR, month, week, number_of_flights, count, flight_id)
        count = count + 1
        
        if DEPARTURE:
            first_point_outside_circle_index = get_first_point_outside_circle(flight_id, new_df)
            
            # last point might be within circle with big radius
            #if first_point_outside_circle_index == -1:
            #    continue
            
            new_df_inside_circle = new_df.iloc[:first_point_outside_circle_index+1].copy()
        
        else:
            first_point_inside_circle_index = get_first_point_inside_circle(flight_id, new_df)
            
            if first_point_inside_circle_index == -1:
                continue
            
            if first_point_inside_circle_index == 0:
                last_point_outside_circle_index = 0
            else:
                last_point_outside_circle_index = first_point_inside_circle_index - 1
            
            new_df_inside_circle = new_df.iloc[last_point_outside_circle_index:].copy()
            
            # reassign sequence
            new_df_inside_circle.reset_index(drop=False, inplace=True)
            new_df_inside_circle_length = len(new_df_inside_circle)
            
            sequence_list = list(range(new_df_inside_circle_length))
            
            new_df_inside_circle.drop(['sequence'], axis=1, inplace=True)
            
            new_df_inside_circle['sequence'] = sequence_list
            new_df_inside_circle.set_index(['flightId', 'sequence'], drop=True, inplace=True)
        
        tracks_inside_circle_df = tracks_inside_circle_df.append(new_df_inside_circle)
        
    tracks_inside_circle_df.to_csv(os.path.join(OUTPUT_DIR, csv_output_file), sep=' ', encoding='utf-8', float_format='%.6f', header=None, index=True)


# for arrivals
def get_first_point_inside_circle(flight_id, new_df):
    
    lat = 0.0
    lon = 0.0
    for seq, row in new_df.groupby(level='sequence'):
        lat = row.loc[(flight_id, seq)]['lat']
        lon = row.loc[(flight_id, seq)]['lon']
        if check_circle_contains_point((lat, lon)):
            return seq
    #print(lat, lon)
    return -1

# for departures
def get_first_point_outside_circle(flight_id, new_df):
    
    lat = 0.0
    lon = 0.0
    for seq, row in new_df.groupby(level='sequence'):
        lat = row.loc[(flight_id, seq)]['lat']
        lon = row.loc[(flight_id, seq)]['lon']
        if not check_circle_contains_point((lat, lon)):
            return seq
    print(lat, lon)
    return -1


def check_circle_contains_point(point):
    central_point = (CENTRAL_LAT, CENTRAL_LON)
    distance = geodesic(central_point, point).meters
    
    if distance < RADIUS*1852:
        return True
    else:
        return False


def extract_TMA_part(month, week):
    
    input_filename = AIRPORT_ICAO + '_tracks_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
    if DEPARTURE:
        input_filename = 'osn_departure_' + input_filename
    else:
        input_filename = 'osn_arrival_' + input_filename

    all_tracks_df = get_all_tracks(input_filename)
    
    output_filename = AIRPORT_ICAO + '_tracks_' + area  + '_'+ YEAR + '_' + month + '_week' + str(week) + '.csv'
    if DEPARTURE:
        output_filename = 'osn_departure_' + output_filename
    else:
        output_filename = 'osn_arrival_' + output_filename
        
    if AREA=="TMA":
        get_tracks_inside_TMA(month, week, all_tracks_df, output_filename)
    else:
        get_tracks_inside_circle(month, week, all_tracks_df, output_filename)
        
        

import time
start_time = time.time()

from multiprocessing import Process

if __name__ == '__main__':
    for month in MONTHS:

        procs = [] 
    
        for week in WEEKS:
        
            if week == 5 and month == '02' and not calendar.isleap(int(YEAR)):
                continue
        
            proc = Process(target=extract_TMA_part, args=(month, week,))
            procs.append(proc)
            proc.start()
        
        # complete the processes
        for proc in procs:
            proc.join()
            
print((time.time()-start_time)/60)
