from config import *
import pandas as pd
import numpy as np
import calendar
from scipy.signal import medfilt

import warnings
warnings.filterwarnings('ignore')

import os

DATA_DIR = os.path.join("..", "Data")
DATA_DIR = os.path.join(DATA_DIR, AIRPORT_ICAO)
DATA_DIR = os.path.join(DATA_DIR, YEAR)

area = ("around" + str(RADIUS) + "NM", "aroundTMA")[AREA == "TMA"]
INPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + "_" + YEAR + "_downloaded")
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + "_" + YEAR + "_fixed_lat_lon")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

    
import time
start_time = time.time()

flight_type = "Departure" if DEPARTURE else "Arrival"


for month in MONTHS:
    
    for week in WEEKS:
        
        if week == 5 and month == '02' and not calendar.isleap(int(YEAR)):
            continue
        
        filename = AIRPORT_ICAO + '_states_' + area + '_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
        if DEPARTURE:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_arrival_' + filename
        
        full_filename = os.path.join(INPUT_DIR, filename)
        
        
        df = pd.read_csv(full_filename, sep=' ',
            names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'velocity', 'beginDate', 'endDate'],
            dtype={'sequence':int, 'timestamp':int, 'rawAltitude':int, 'velocity':int, 'beginDate':str, 'endDate':str})

        df.set_index(['flightId', 'sequence'], inplace = True)

        number_of_flights = len(df.groupby(level='flightId'))
        
        new_df = pd.DataFrame(columns=['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'velocity', 'beginDate', 'endDate'],
            dtype=str)

        count = 0
        
        for flight_id, flight_df in df.groupby(level='flightId'):
            
            count = count + 1
            print("STEP4", flight_type, area, AIRPORT_ICAO, YEAR, month, week, number_of_flights, count, flight_id)
            
            flight_states_df = flight_df.copy()
            
            number_of_points = len(flight_states_df)
            
            if not flight_states_df.empty:
                
                lats = list(flight_df['lat'])
                lons = list(flight_df['lon'])
                
                lats = flight_states_df["lat"].values
                lons = flight_states_df["lon"].values
                                
                flight_states_df["lat"] = medfilt(lats,11)
                flight_states_df["lon"] = medfilt(lons,11)
            
            flight_states_df.reset_index(drop = False, inplace = True)
            flight_states_df = flight_states_df[['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'velocity', 'beginDate', 'endDate']]
            
            new_df = new_df.append(flight_states_df)
            
        filename = AIRPORT_ICAO + '_states_' + area + '_fixed_lat_lon_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
        
        if DEPARTURE:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_arrival_' + filename

        full_filename = os.path.join(OUTPUT_DIR, filename)
        
        new_df.to_csv(full_filename, sep=' ', encoding='utf-8', float_format='%.6f', header=False, index=False)

print((time.time()-start_time)/60)