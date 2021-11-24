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
from airports.constants_ESSA import *

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

rwy08_azimuth, rwy26_azimuth, distance = geod.inv(rwy08_lat[0], rwy08_lon[0], rwy08_lat[1], rwy08_lon[1])
rwy01L_azimuth, rwy19R_azimuth, distance = geod.inv(rwy01L_lat[0], rwy01L_lon[0], rwy01L_lat[1], rwy01L_lon[1])
rwy01R_azimuth, rwy19L_azimuth, distance = geod.inv(rwy01R_lat[0], rwy01R_lon[0], rwy01R_lat[1], rwy01R_lon[1])

#print(rwy08_azimuth, rwy26_azimuth, rwy01L_azimuth, rwy19R_azimuth, rwy01R_azimuth, rwy19L_azimuth)
# ~ 7 -173 70 -110 70 -110

a = (rwy01L_lon[0], rwy01L_lat[0])
b = (rwy01L_lon[1], rwy01L_lat[1])
# distance between runways - 2 km
offset_length = 0.018 #approximate 1 km in longitude degrees, when latitude is 60N

ab = LineString([a, b])
cd = ab.parallel_offset(offset_length)
Point1 = cd.boundary[0]
Point2 = cd.boundary[1]
print(Point1, Point2)


# Check the sign of the determninant of 
# | x2-x1  x3-x1 |
# | y2-y1  y3-y1 |
# It will be positive for points on one side, and negative on the other (0 on the line)
def is_left(Point3):
    if ((Point2.x - Point1.x)*(Point3.y - Point1.y) - (Point2.y - Point1.y)*(Point3.x - Point1.x)) < 0:
        return True
    else:
        return False


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
        # 30 seconds before:
        trajectory_point_before_last = [flight_df['lat'][-30], flight_df['lon'][-30]]
        
        #fwd_azimuth, back_azimuth, distance = geod.inv(lat1, long1, lat2, long2)
        trajectory_azimuth, temp1, temp2 = geod.inv(trajectory_point_before_last[0],
                                                    trajectory_point_before_last[1],
                                                    trajectory_point_last[0],
                                                    trajectory_point_last[1])

        if (trajectory_azimuth > -50) and (trajectory_azimuth < 40):
            runway = '08'
        elif ((trajectory_azimuth > -180) and (trajectory_azimuth < -140)) or ((trajectory_azimuth > 130) and (trajectory_azimuth < 180)):
            runway = '26'
        elif (trajectory_azimuth > 40) and (trajectory_azimuth < 130): #01L or 01R
            Point3 = Point(trajectory_point_last[1], trajectory_point_last[0])
            if is_left(Point3):
                runway = '01L'
            else:
                runway = '01R'
        else: # 19L or 19R
            Point3 = Point(trajectory_point_last[1], trajectory_point_last[0])
            if is_left(Point3):
                runway = '19R'
            else:
                runway = '19L'
        
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

    rwy01L_df = pd.DataFrame()
    rwy19R_df = pd.DataFrame()
    rwy01R_df = pd.DataFrame()
    rwy19L_df = pd.DataFrame()
    rwy08_df = pd.DataFrame()
    rwy26_df = pd.DataFrame()
    
    count = 0
    for flight_id, flight_df in states_df.groupby(level='flightId'):
        count = count + 1
        print("create runways files", AIRPORT_ICAO, YEAR, month, week, number_of_flights, count, flight_id)

        runway = runways_df.loc[flight_id][['runway']].values[0]
    
        if runway == "01L":
            rwy01L_df = rwy01L_df.append(flight_df)
        elif runway == "19R":
            rwy19R_df = rwy19R_df.append(flight_df)
        elif runway == "01R":
            rwy01R_df = rwy01R_df.append(flight_df)
        elif runway == "19L":
            rwy19L_df = rwy19L_df.append(flight_df)
        elif runway == "08":
            rwy08_df = rwy08_df.append(flight_df)
        else:
            rwy26_df = rwy26_df.append(flight_df)

    WEEK_OUTPUT_DIR = os.path.join(RUNWAYS_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + \
        "_" + YEAR + "_" + month + "_week" + str(week) + "_by_runways")

    if not os.path.exists(WEEK_OUTPUT_DIR):
        os.makedirs(WEEK_OUTPUT_DIR)
        
    output_filename = "osn_arrival_" + AIRPORT_ICAO + '_states_'+ area +'_' + YEAR + '_' + \
        month + '_week' + str(week)    
    full_output_filename = os.path.join(WEEK_OUTPUT_DIR, output_filename)

    rwy01L_df.to_csv(full_output_filename + "_rwy01L.csv", sep=' ', encoding='utf-8', float_format='%.3f', index = True, header = False)
    rwy19R_df.to_csv(full_output_filename + "_rwy19R.csv", sep=' ', encoding='utf-8', float_format='%.3f', index = True, header = False)
    rwy01R_df.to_csv(full_output_filename + "_rwy01R.csv", sep=' ', encoding='utf-8', float_format='%.3f', index = True, header = False)
    rwy19L_df.to_csv(full_output_filename + "_rwy19L.csv", sep=' ', encoding='utf-8', float_format='%.3f', index = True, header = False)
    rwy08_df.to_csv(full_output_filename + "_rwy08.csv", sep=' ', encoding='utf-8', float_format='%.3f', index = True, header = False)
    rwy26_df.to_csv(full_output_filename + "_rwy26.csv", sep=' ', encoding='utf-8', float_format='%.3f', index = True, header = False)
  
    
def main():
    
    for month in MONTHS:
        
        for week in WEEKS:
        
            if week == 5 and month == '02' and not calendar.isleap(int(YEAR)):
                continue
        
            determine_runways(month, week)
                       
            create_runways_files(month, week)
    
    
main()    

print("--- %s minutes ---" % ((time.time() - start_time)/60))