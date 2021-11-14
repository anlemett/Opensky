from config import *

import os

DATA_DIR = os.path.join("..", "Data")
DATA_DIR = os.path.join(DATA_DIR, AIRPORT_ICAO)
DATA_DIR = os.path.join(DATA_DIR, YEAR)

area = (str(RADIUS) + "NM", "TMA")[AREA == "TMA"]
INPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + '_' + YEAR + "_smoothed")
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + '_' + YEAR)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


# flights with the last altitude value less than this value are considered as 
# landed and with complete data
descent_end_altitude = 600 #meters

climb_first_altitude = 1000 # meters
climb_end_altitude = 2000 #meters

import pandas as pd
import numpy as np
import calendar

import time
start_time = time.time()

flight_type = "Departure" if DEPARTURE else "Arrival"


for month in MONTHS:
    
    for week in WEEKS:
        
        if week == 5 and month == '02' and not calendar.isleap(int(YEAR)):
            continue
        
        filename = AIRPORT_ICAO + '_states_' + area + '_smoothed_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
        
        if DEPARTURE:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_arrival_' + filename
        
        full_filename = os.path.join(INPUT_DIR, filename)
        
        df = pd.read_csv(full_filename, sep=' ',
            names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'beginDate', 'endDate'],
            dtype={'sequence':int, 'timestamp':int, 'rawAltitude':int, 'altitude':int, 'beginDate':str, 'endDate':str})
        
        df.set_index(['flightId', 'sequence'], inplace = True)
        
        number_of_flights = len(df.groupby(level='flightId'))
        count = 0
        
        for flight_id, flight_id_group in df.groupby(level='flightId'):
            
            count = count + 1
            print("STEP8", flight_type, area, AIRPORT_ICAO, YEAR, month, week, number_of_flights, count, flight_id)
            
            if DEPARTURE:
                
                ###################################################################
                # Last altitude too small (incomplete data or bad smoothing):
                ###################################################################
                
                altitudes = flight_id_group['altitude']
                last_height = altitudes.tolist()[-1]
                
                if last_height < climb_end_altitude:
                    df = df.drop(flight_id)
                    continue
                
                ###################################################################
                # First altitude too big (incomplete data):
                
                altitudes = flight_id_group['altitude']
                first_height = altitudes.tolist()[0]
                
                if first_height > climb_first_altitude:
                    df = df.drop(flight_id)
                    continue
            else:
                ###################################################################
                # Last altitude too big (incomplete data or bad smoothing):
                ###################################################################
                    
                altitudes = flight_id_group['altitude']
                last_height = altitudes.tolist()[-1]
                
                if last_height > descent_end_altitude:
                    df = df.drop(flight_id)
                    continue
                
                ###################################################################
                # First altitude too small (departure and arrival at the same airport):
                ###################################################################
                
                altitudes = flight_id_group['altitude']
                first_height = altitudes.tolist()[0]
                
                if first_height < descent_end_altitude:
                    df = df.drop(flight_id)
                    continue
                    
        filename = AIRPORT_ICAO + '_states_' + area + '_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
        
        if DEPARTURE:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_arrival_' + filename

        full_filename = os.path.join(OUTPUT_DIR, filename)
        
        df.to_csv(full_filename, sep=' ', encoding='utf-8', float_format='%.6f', header=False, index=True)

print((time.time()-start_time)/60)