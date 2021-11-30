from config import *

# Threshold for lat/lon fluctuattion
# If the threshold is too big, small fluctuations will be skiped
# If the threshold is too small, the real value might be treated as fluctuation, hence the whole trajectory is messed up
threshold1 = 0.1 # degree
# This threshold helps to fix 0 lat and lon (might be helpful if TMA close to 0 parallel or medidian) 
threshold2 = 10 # degree

import os

DATA_DIR = os.path.join("..", "Data")
DATA_DIR = os.path.join(DATA_DIR, AIRPORT_ICAO)
DATA_DIR = os.path.join(DATA_DIR, YEAR)

area = ("around" + str(RADIUS) + "NM", "aroundTMA")[AREA == "TMA"]
INPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + "_" + YEAR)
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + "_" + YEAR + "_fixed_lat_lon")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

import pandas as pd
import numpy as np
import calendar

central_lat = min(TMA_lat) + (max(TMA_lat)-min(TMA_lat))/2
central_lon = min(TMA_lon) + (max(TMA_lon)-min(TMA_lon))/2
    
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
                
                number_of_points = len(lats)
                
                first_good_point_index = 0
                while (abs(abs(lats[first_good_point_index]) - abs (central_lat)) > threshold2) and \
                      (abs(abs(lons[first_good_point_index]) - abs (central_lon)) > threshold2):
                    first_good_point_index = first_good_point_index + 1

                for lat_lon_index in range(0,first_good_point_index):
                    lats[lat_lon_index] = lats[first_good_point_index]
                    lons[lat_lon_index] = lons[first_good_point_index]
                    
                prev_lat = lats[first_good_point_index]
                prev_lon = lons[first_good_point_index]
                
                for i in range(first_good_point_index+1, number_of_points): 
                    
                    shift = 0
                    too_long_shift = False # if the latitudes are the same for some time (error) and then we get the right latitude but the difference is bigger than threshold1
                    
                    while (((abs(abs(lats[i+shift]) - abs (prev_lat)) > threshold1) and not too_long_shift or \
                            (abs(abs(lats[i+shift]) - abs (prev_lat)) == 0)) ) or (abs(abs(lats[i+shift]) - abs (central_lat)) > threshold2):
                        shift = shift + 1
                        if shift > 10:
                            too_long_shift = True
                        if i+shift == number_of_points:
                            break
                    
                    if (i+shift < number_of_points):
                        lats_step = (lats[i+shift] - prev_lat)/(shift + 1)
                        while (shift > 0):
                            next_lat = lats[i+shift]
                            shift = shift - 1
                            lats[i+shift] = next_lat - lats_step
                    elif i > 1:
                        lats_step = (lats[i-1] - lats[i-2])/2
                        while (shift > 0):
                            shift = shift - 1
                            lats[i+shift] = lats[i-1] + (shift + 1)*lats_step
                            
                    prev_lat = lats[i]
                
                
                for i in range(first_good_point_index, number_of_points):
                    
                    shift = 0
                    too_long_shift = False # if the longitudes are the same for some time (error) and then we get the right longitude but the difference is bigger than threshold
                    
                    while ((i+shift < number_of_points-1) and ((abs(abs(lons[i+shift]) - abs (prev_lon)) > threshold1) and not too_long_shift or \
                            (abs(abs(lons[i+shift]) - abs (prev_lon)) == 0))) or (abs(abs(lons[i+shift]) - abs (central_lon)) > threshold2):
                        shift = shift + 1
                        if shift > 10:
                            too_long_shift = True
                        if i+shift == number_of_points:
                            break

                    
                    if (i+shift < number_of_points):
                        lons_step = (lons[i+shift] - prev_lon)/(shift + 1)
                        while (shift > 0):
                            next_lon = lons[i+shift]
                            shift = shift - 1
                            lons[i+shift] = next_lon - lons_step
                    elif i > 1:
                        lons_step = (lons[i-1] - lons[i-2])/2
                        while (shift > 0):
                            shift = shift - 1
                            lons[i+shift] = lons[i-1] + (shift + 1)*lons_step
                    prev_lon = lons[i]
                    
                flight_states_df["lat"] = lats
                flight_states_df["lon"] = lons
            
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