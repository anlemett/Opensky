from config import *

import pandas as pd
import os
import pyproj
from shapely.geometry import Point
from shapely.geometry import LineString
from datetime import datetime
import calendar
import sys

# append the path of the parent directory
sys.path.append("..")
from airports.constants_ESGG import *

import time
start_time = time.time()

DATA_DIR = os.path.join("..", "Data")
DATA_DIR = os.path.join(DATA_DIR, AIRPORT_ICAO)
DATA_DIR = os.path.join(DATA_DIR, YEAR)

area = (str(RADIUS) + "NM", "TMA")[AREA == "TMA"]
INPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + '_' + YEAR)
RUNWAYS_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + '_' + YEAR + "_by_runways")

if not os.path.exists(RUNWAYS_DIR):
   os.makedirs(RUNWAYS_DIR)

geod = pyproj.Geod(ellps='WGS84')   # to determine runways via azimuth
#fwd_azimuth, back_azimuth, distance = geod.inv(lat1, long1, lat2, long2)

rwy03_azimuth, rwy21_azimuth, distance = geod.inv(rwy03_lat[0], rwy03_lon[0], rwy03_lat[1], rwy03_lon[1])

#print(rwy03_azimuth, rwy21_azimuth)
# ~ 47.233 -132.761

def get_all_states(input_filename):

    df = pd.read_csv(input_filename, sep=' ',
        names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'rawAltitude', 'altitude', 'velocity', 'beginDate', 'endDate'],
        dtype={'flightId':str, 'sequence':int, 'timestamp':int, 'lat':float, 'lon':float, 'rawAltitude':int, 'altitude':float, 'velocity':float, 'beginDate':str, 'endDate':str})
    
    df.set_index(['flightId', 'sequence'], inplace=True)
    
    return df


def determine_runways(month, week):
    
    input_filename = "osn_arrival_" + AIRPORT_ICAO + "_states_"+  area + "_" + YEAR + \
        "_" + month + "_week" + str(week) + ".csv"
    full_input_filename = os.path.join(INPUT_DIR, input_filename)
         
    output_filename = "osn_arrival_" + AIRPORT_ICAO + "_runways_" + YEAR + '_' + \
        month + "_week" + str(week) + ".csv"
    full_output_filename = os.path.join(RUNWAYS_DIR, output_filename)

    states_df = get_all_states(full_input_filename)
    
    number_of_flights = len(states_df.groupby(level='flightId'))
    print(number_of_flights)
        
    runways_df = pd.DataFrame(columns=['flightId', 'date', 'hour', 'runway'])
    
    count = 0
    for flight_id, flight_df in states_df.groupby(level='flightId'):
        
        count = count + 1
        print("determine runways", AIRPORT_ICAO, YEAR, month, week, number_of_flights, count, flight_id)
        
        date_str = states_df.loc[flight_id].head(1)['endDate'].values[0]
        
        end_timestamp = states_df.loc[flight_id]['timestamp'].values[-1]
        end_datetime = datetime.utcfromtimestamp(end_timestamp)
        end_hour_str = end_datetime.strftime('%H')
        
        # Determine Runway based on lat, lon
        
        runway = ""
        trajectory_point_last = [flight_df['lat'][-1], flight_df['lon'][-1]]
        # 20 seconds before:
        trajectory_point_before_last = [flight_df['lat'][-20], flight_df['lon'][-20]]
        
        #fwd_azimuth, back_azimuth, distance = geod.inv(lat1, long1, lat2, long2)
        trajectory_azimuth, temp1, temp2 = geod.inv(trajectory_point_before_last[0],
                                                    trajectory_point_before_last[1],
                                                    trajectory_point_last[0],
                                                    trajectory_point_last[1])

        if (trajectory_azimuth > -43) and (trajectory_azimuth < 137):
            runway = '03'
        else:
            runway = '21'
        #print(runway)

        runways_df = runways_df.append({'flightId': flight_id, 'date': date_str,
                                        'hour': end_hour_str,
                                        'runway': runway}, ignore_index=True)

    runways_df.to_csv(full_output_filename, sep=' ', encoding='utf-8', float_format='%.3f', header=True, index=False)


def create_runways_files(month, week):
    
    states_filename = "osn_arrival_" + AIRPORT_ICAO + '_states_'+ area +'_' + YEAR + '_' + \
        month + '_week' + str(week) + '.csv'

    full_states_filename = os.path.join(INPUT_DIR, states_filename)
    
    states_df = get_all_states(full_states_filename)
    
    number_of_flights = len(states_df.groupby(level='flightId'))
    
    runways_filename = "osn_arrival_" + AIRPORT_ICAO + "_runways_" + YEAR + '_' + \
        month + "_week" + str(week) + ".csv"
    full_runways_filename = os.path.join(RUNWAYS_DIR, runways_filename)
    
    runways_df = pd.read_csv(full_runways_filename, sep=' ')
    runways_df.set_index(['flightId'], inplace=True)

    rwy03_df = pd.DataFrame()
    rwy21_df = pd.DataFrame()

    count = 0
    for flight_id, flight_df in states_df.groupby(level='flightId'):
        
        count = count + 1
        print("create runways files", AIRPORT_ICAO, YEAR, month, week, number_of_flights, count, flight_id)

        runway = runways_df.loc[flight_id][['runway']].values[0]

        if runway == "03":
            rwy03_df = rwy03_df.append(flight_df)
        else:
            rwy21_df = rwy21_df.append(flight_df)

    WEEK_OUTPUT_DIR = os.path.join(RUNWAYS_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + \
        "_" + YEAR + "_" + month + "_week" + str(week) + "_by_runways")

    if not os.path.exists(WEEK_OUTPUT_DIR):
        os.makedirs(WEEK_OUTPUT_DIR)
        
    output_filename = "osn_arrival_" + AIRPORT_ICAO + '_states_'+ area +'_' + YEAR + '_' + \
        month + '_week' + str(week)    
    full_output_filename = os.path.join(WEEK_OUTPUT_DIR, output_filename)

    rwy03_df.to_csv(full_output_filename + "_rwy03.csv", sep=' ', encoding='utf-8', float_format='%.3f', index = True, header = False)
    rwy21_df.to_csv(full_output_filename + "_rwy21.csv", sep=' ', encoding='utf-8', float_format='%.3f', index = True, header = False)
    
    
def main():
 
    for month in MONTHS:
        
        for week in WEEKS:
        
            if week == 5 and month == '02' and not calendar.isleap(int(YEAR)):
                continue
        
            determine_runways(month, week)
                       
            create_runways_files(month, week)
    
main()    

print("--- %s minutes ---" % ((time.time() - start_time)/60))