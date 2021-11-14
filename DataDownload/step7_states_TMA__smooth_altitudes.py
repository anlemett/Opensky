from config import *

# maximum alitude (ceiling) arrival aircraft enter the TMA
# needed to determine altitude fluctuation at the first point
TMA_altitude_threshold = 9000

# Alternative solution - make TMA_altitude_threshold different for diferent airports
# If TMA_altitude_threshold is too big, the fluctuation at first point might be treated as the normal altitude, then the next normal
# altitude is considered as fluctuation, as a result we get wrong vertical profile (horizontal line). The flight might be filtered out
# using the last point altitude (next step).
# If TMA_altitude_threshold is too small, the normal (big) altitude might be treated as fluctuation, then the first altitude below
# the threshold is treated as the normal one, as a result we get fake level for all altitudes above the threshold.
# For statistics the first approach is better.

# Fake levels are still possible due to fluctuation down at the first point (not filtered out).

# threshold for altitude fluctuation toward bigger values
altitude_fluctuation_threshold_up = 300

# If altitude_fluctuation_threshold_up is too big a lot of fluctuations will be skiped (then vertical profile is changed greatly by 
# smoothing procedure).
# Too small value might cause a fake level but in very rare cases(arrivals): wrong altitude for some time (the same altitude) and then the aircraft
# really goes up.

# threshold for altitude fluctuation toward smaller values
if DEPARTURE:
    altitude_fluctuation_threshold_down = 300
else:
    altitude_fluctuation_threshold_down = 600

# If altitude_fluctuation_threshold_down is too big  a lot of fluctuations will be skiped (then vertical profile is changed greatly by 
# smoothing procedure).
# Too small value might cause a fake level in the following cases(arrivals): wrong altitude data for some time (the same altitude), then correct
# altitude, but the difference between this altittude and the previous correct altitude is above the fluctuation down threshold. Flights 
# of this kind are also filtered out using the last point altitude (next step).


import os

DATA_DIR = os.path.join("..", "Data")
DATA_DIR = os.path.join(DATA_DIR, AIRPORT_ICAO)
DATA_DIR = os.path.join(DATA_DIR, YEAR)

area = (str(RADIUS) + "NM", "TMA")[AREA == "TMA"]
INPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + '_' + YEAR + "_filtered")
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + '_' + YEAR + "_smoothed")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


import pandas as pd
import calendar
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

        df.set_index(['flightId', 'sequence'], inplace = True)
        
        #print(df.head(1))

        number_of_flights = len(df.groupby(level='flightId'))
        count = 0

        for flight_id, flight_id_group in df.groupby(level='flightId'):
            
            count = count + 1
            
            print("STEP7", flight_type, area, AIRPORT_ICAO, YEAR, month, week, number_of_flights, count, flight_id)
            
            below_TMA_altitude_threshold_df = flight_id_group[(flight_id_group['rawAltitude'] > 0) & (flight_id_group['rawAltitude'] < TMA_altitude_threshold)]
            
            if below_TMA_altitude_threshold_df.empty:    #flight is in cruise
                continue
            
            flight_states_df = flight_id_group.copy()
            #flight_states_df = df.loc[(flight_id, ), :]
            
            # PHASE 1 Substitute big fluctuations with neighbor values
            
            flight_states_df.reset_index(drop = False, inplace = True)
            df_len = len(flight_states_df)
            flight_states_df.set_index('sequence', inplace=True)
            
            if not flight_states_df.empty:
                
                opensky_states_altitudes = []
                opensky_states_times = []
                opensky_states_fixed_altitudes = []
                
                i = 0
                
                prev_altitude = list(flight_states_df['rawAltitude'])[i]
                
                while (prev_altitude==0) or (prev_altitude > TMA_altitude_threshold): #fluctuation
                    i = i + 1
                    prev_altitude = list(flight_states_df['rawAltitude'])[i]
                
                ii = i
                while i>0:
                    i = i - 1
                    opensky_states_fixed_altitudes.append(prev_altitude)
                
                for seq, row in flight_states_df.iterrows():
                    
                    if seq < ii:
                        continue
                    
                    opensky_states_altitudes.append(row['rawAltitude'])
                    
                    if (row['rawAltitude'] < prev_altitude) & (prev_altitude-row['rawAltitude'] > altitude_fluctuation_threshold_down):
                        opensky_states_fixed_altitudes.append(prev_altitude)
                        continue
                   
                    if (row['rawAltitude'] > prev_altitude) & (row['rawAltitude'] - prev_altitude > altitude_fluctuation_threshold_up):
                        opensky_states_fixed_altitudes.append(prev_altitude)
                        continue
                    
                    opensky_states_fixed_altitudes.append(row['rawAltitude'])
                    
                    prev_altitude = row['rawAltitude']
                    
                flight_states_df["altitude"] = opensky_states_fixed_altitudes
                
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
            
            #print(new_df.head(1))
        
        
        filename = AIRPORT_ICAO + '_states_' + area + '_smoothed_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
        
        if DEPARTURE:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_arrival_' + filename
        
        full_filename = os.path.join(OUTPUT_DIR, filename)
        
        new_df.to_csv(full_filename, sep=' ', encoding='utf-8', float_format='%.6f', header=False, index=False)

print((time.time()-start_time)/60)
