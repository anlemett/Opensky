from config import *

import warnings
warnings.filterwarnings('ignore')


import os

DATA_DIR = os.path.join("..", "Data")
DATA_DIR = os.path.join(DATA_DIR, AIRPORT_ICAO)
DATA_DIR = os.path.join(DATA_DIR, YEAR)

area = (str(RADIUS) + "NM", "TMA")[AREA == "TMA"]
INPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + '_' + YEAR + "_filtered_by_callsign")
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + '_' + YEAR + "_smoothed")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


import calendar
import pandas as pd
import numpy as np
from scipy.signal import medfilt
from scipy.ndimage import gaussian_filter1d

import time
start_time = time.time()

flight_type = "Departure" if DEPARTURE else "Arrival"


for month in MONTHS:
    
    for week in WEEKS:
        
        if week == 5 and month == '02' and not calendar.isleap(int(YEAR)):
            continue
        
        filename = AIRPORT_ICAO + '_states_' + area +'_filtered_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
        
        if DEPARTURE:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_arrival_' + filename
        
        full_filename = os.path.join(INPUT_DIR, filename)
        
        
        df = pd.read_csv(full_filename, sep=' ',
            names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'velocity', 'beginDate', 'endDate'],
            index_col=False,
            dtype={'sequence':int, 'timestamp':int, 'rawAltitude':int, 'beginDate':str, 'endDate':str})
        
        new_df = pd.DataFrame(columns=['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'beginDate', 'endDate'],
                              dtype=str)
        
        dropped_df = pd.DataFrame(columns=['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'beginDate', 'endDate'],
                              dtype=str)
        dropped_df.set_index(['flightId', 'sequence'], inplace=True)

        df.set_index(['flightId', 'sequence'], inplace = True)
        
        number_of_flights = len(df.groupby(level='flightId'))
        count = 0

        for flight_id, flight_id_group in df.groupby(level='flightId'):
            
            count = count + 1
            
            print("STEP7", flight_type, area, AIRPORT_ICAO, YEAR, month, week, number_of_flights, count, flight_id)
            
            flight_states_df = flight_id_group.copy()
            
            #flight_states_df = df.loc[(flight_id, ), :]
            
            # PHASE 1 Substitute spikes with median of neighbors 
            
            flight_states_df.reset_index(drop = False, inplace = True)
            df_len = len(flight_states_df)
            
            flight_states_df.set_index('sequence', inplace=True)
            
            if not flight_states_df.empty:
                
                altitudes = flight_states_df["rawAltitude"].values
                                
                flight_states_df["altitude"] = medfilt(altitudes,11)
                
            flight_states_df.reset_index(drop = False, inplace = True)
            flight_states_df.set_index(['flightId', 'sequence'], inplace=True)
            
            
            # PHASE 2 Use Gaussian filter to smooth 'stairs'
            
            y = list(flight_states_df["altitude"])
            
            # Smooth with a gaussian filter
            smooth_y = gaussian_filter1d(y, 10)
            
            flight_states_df["altitude"] = smooth_y
            
            flight_states_df = flight_states_df.reset_index(drop=False)
            
            flight_states_df = flight_states_df[['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'beginDate', 'endDate']]
            
            new_df = new_df.append(flight_states_df)
        
        
        filename = AIRPORT_ICAO + '_states_' + area + '_smoothed_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
        
        if DEPARTURE:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_arrival_' + filename
        
        full_filename = os.path.join(OUTPUT_DIR, filename)
        
        new_df.to_csv(full_filename, sep=' ', encoding='utf-8', float_format='%.6f', header=False, index=False)
        
print((time.time()-start_time)/60)
