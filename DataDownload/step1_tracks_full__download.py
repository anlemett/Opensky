from config import *

from datetime import datetime
from datetime import timezone
import calendar

import os

DATA_DIR = os.path.join("..", "Data")

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
    
DATA_DIR = os.path.join(DATA_DIR, AIRPORT_ICAO)

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

DATA_DIR = os.path.join(DATA_DIR, YEAR)

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_tracks_" + YEAR)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

import requests
import math
import pandas as pd


log_filename = "dropped_flights_" + YEAR + '.txt'
full_log_filename = os.path.join(OUTPUT_DIR, log_filename)

flight_type = "Departure" if DEPARTURE else "Arrival"

from opensky_credentials import USERNAME, PASSWORD

class LiveDataRetriever:
    API_URL = 'https://opensky-network.org/api'

    AUTH_DATA = (USERNAME, PASSWORD)

    def get_list_of_arriving_aircraft(self, timestamp_begin, timestamp_end):

        flights_url = self.API_URL + '/flights/arrival'

        request_params = {
            'airport': AIRPORT_ICAO,
            'begin': timestamp_begin,
            'end': timestamp_end
        }
        
        while True:
            try:
                print("request")
                res = requests.get(flights_url, params=request_params, auth=self.AUTH_DATA).json()
                break
            except Exception as str_error:
                print("Exception: ")
                print(str_error)
                pass

        #print(res)
        return res

    def get_list_of_departure_aircraft(self, timestamp_begin, timestamp_end):

        flights_url = self.API_URL + '/flights/departure'

        request_params = {
            'airport': AIRPORT_ICAO,
            'begin': timestamp_begin,
            'end': timestamp_end
        }
        
        while True:
            try:
                res = requests.get(flights_url, params=request_params, auth=self.AUTH_DATA).json()
                break
            except Exception as str_error:
                print("Exception: ")
                print(str_error)
                pass

        return res

    def get_track_data(self, flight_icao24, flight_time):
        track_data_url = self.API_URL + '/tracks/all'

        request_params = {
            'time': flight_time,
            'icao24': flight_icao24
        }

        return requests.get(track_data_url, params=request_params, auth=self.AUTH_DATA).json()


# API does not allow longer than 7 days time periods
def get_tracks_data(data_retriever, flights, date_time_begin, date_time_end, month, week):
    
    dropped_flights_file = open(full_log_filename, 'a+')
    
    number_of_flights = len(flights)

    new_data = []

    dropped_flights_icao = 0
    dropped_flights_first_seen = 0
    dropped_flights_last_seen = 0
    dropped_flights_callsign = 0

    for i in range(number_of_flights):

        print("STEP1", flight_type, AIRPORT_ICAO, YEAR, month, week, number_of_flights, i+1)

        if flights[i] == 'Start after end time or more than seven days of data requested': #ESSA 22.10.2018-29.10.2018 -> 28.10 to 5th week
            print(flights[i])
            continue

        if flights[i]['icao24'] is None:
            dropped_flights_icao = dropped_flights_icao + 1
            continue

        if flights[i]['firstSeen'] is None:
            dropped_flights_first_seen = dropped_flights_first_seen + 1
            continue

        if flights[i]['lastSeen'] is None:
            dropped_flights_last_seen = dropped_flights_last_seen + 1
            continue
 
        while True:
            try:
                d = data_retriever.get_track_data(flights[i]['icao24'], math.ceil((flights[i]['firstSeen']+flights[i]['lastSeen'])/2))
                break
            except Exception as str_error:
                print("Exception: ")
                print(str_error)
                pass
            
        sequence = 0
        
        if (flights[i]['callsign'] is None) and (d['callsign'] is None):
            dropped_flights_callsign = dropped_flights_callsign + 1
            continue

        for element in d['path']:
            new_d = {}


            if DEPARTURE:
                if flights[i]['estArrivalAirport'] is None:
                    new_d['destination'] = 'NaN'
                else:
                    new_d['destination'] = flights[i]['estArrivalAirport']
                
                begin_timestamp = d['startTime']
                begin_datetime = datetime.utcfromtimestamp(begin_timestamp)
                new_d['beginDate'] = begin_datetime.strftime('%y%m%d')

            else:
                if flights[i]['estDepartureAirport'] is None:
                    new_d['origin'] = 'NaN'
                else:
                    new_d['origin'] = flights[i]['estDepartureAirport']

                end_timestamp = d['endTime']
                end_datetime = datetime.utcfromtimestamp(end_timestamp)
                new_d['endDate'] = end_datetime.strftime('%y%m%d')

            new_d['sequence'] = sequence
            sequence = sequence + 1

            el_timestamp = element[0]    #time
            el_datetime = datetime.utcfromtimestamp(el_timestamp)

            #new_d['date'] = el_datetime.strftime('%y%m%d')
            #new_d['time'] = el_datetime.strftime('%H%M%S')
            new_d['timestamp'] = el_timestamp
            new_d['lat'] = element[1]
            new_d['lon'] = element[2]
            new_d['baroAltitude'] = element[3]


            new_d['callsign'] = d['callsign'].strip() if d['callsign'] else flights[i]['callsign'].strip()
            new_d['icao24'] = d['icao24'].strip() if d['icao24'] else flights[i]['icao24'].strip()

            new_data.append(new_d)

    print(month, week, file = dropped_flights_file)
    print("dropped_flights_icao", file = dropped_flights_file)
    print(dropped_flights_icao, file = dropped_flights_file)
    print("dropped_flights_first_seen", file = dropped_flights_file)
    print(dropped_flights_first_seen, file = dropped_flights_file)
    print("dropped_flights_last_seen", file = dropped_flights_file)
    print(dropped_flights_last_seen, file = dropped_flights_file)
    print("dropped_flights_callsign", file = dropped_flights_file)
    print(dropped_flights_callsign, file = dropped_flights_file)
    
    dropped_flights_file.close()
    
    if DEPARTURE:
        #data_df = pd.DataFrame(new_data, columns = ['sequence', 'destination', 'beginDate', 'endDate', 'callsign', 'icao24', 'date','time', 'timestamp', 'lat', 'lon', 'baroAltitude'])
        data_df = pd.DataFrame(new_data, columns = ['sequence', 'destination', 'beginDate', 'callsign', 'icao24', 'timestamp', 'lat', 'lon', 'baroAltitude'])
    else:
        #data_df = pd.DataFrame(new_data, columns = ['sequence', 'origin', 'beginDate', 'endDate', 'callsign', 'icao24', 'date','time', 'timestamp', 'lat', 'lon', 'baroAltitude'])
        data_df = pd.DataFrame(new_data, columns = ['sequence', 'origin', 'endDate', 'callsign', 'icao24', 'timestamp', 'lat', 'lon', 'baroAltitude'])

    return data_df


def assign_flight_ids(month, week, tracks_df, output_filename):
    
    if DEPARTURE:
        tracks_df['flight_id'] = tracks_df.apply(lambda row: str(row['beginDate']) + str(row['callsign']), axis = 1) 
    else:
        tracks_df['flight_id'] = tracks_df.apply(lambda row: str(row['endDate']) + str(row['callsign']), axis = 1) 
    
    tracks_df.set_index(['flight_id', 'sequence'], inplace=True)
    
    tracks_df = tracks_df.groupby(level=tracks_df.index.names)
    
    tracks_df = tracks_df.first()
    
    tracks_df.to_csv(output_filename, sep=' ', encoding='utf-8', float_format='%.6f', index=True, header=None)


def download_tracks_week(month, week, date_time_begin, date_time_end):
    
    timestamp_begin = int(date_time_begin.timestamp())   #float -> int
    timestamp_end = int(date_time_end.timestamp())

    data_retriever = LiveDataRetriever()
    
    filename = AIRPORT_ICAO + '_tracks_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
    
    if DEPARTURE:
        flights = data_retriever.get_list_of_departure_aircraft(timestamp_begin, timestamp_end)
        filename = 'osn_departure_' + filename
    else:
        flights = data_retriever.get_list_of_arriving_aircraft(timestamp_begin, timestamp_end)
        filename = 'osn_arrival_' + filename

    if flights:
        opensky_df = get_tracks_data(data_retriever, flights, date_time_begin, date_time_end, month, week)

        #opensky_df = opensky_df.astype({"time": str, "date": str})
        opensky_df.reset_index(drop=True, inplace=True)
    
        output_filename = os.path.join(OUTPUT_DIR, filename)
        assign_flight_ids(month, week, opensky_df, output_filename)
    else:
        print("No flights")

import time
start_time = time.time()

from multiprocessing import Process


if __name__ == '__main__':
    for month in MONTHS:
    
        procs = []
    
        for week in WEEKS:
        
            if week>=1 and week<=4:

                DATE_TIME_BEGIN = datetime(int(YEAR), int(month), (week-1) * 7 + 1, 0, 0, 0, 0, timezone.utc)
                if month == '02' and week == 4 and not calendar.isleap(int(YEAR)):
                    DATE_TIME_END = datetime(int(YEAR), 3, 1, 0, 0, 0, 0)
                else:
                    DATE_TIME_END = datetime(int(YEAR), int(month), week * 7 + 1, 0, 0, 0, 0, timezone.utc)
            
                proc = Process(target=download_tracks_week, args=(month, week, DATE_TIME_BEGIN, DATE_TIME_END,))
                procs.append(proc)
                proc.start()
            
            elif week ==5:

                if month == '02' and not calendar.isleap(int(YEAR)):
                    continue
                elif month == '12':
                    DATE_TIME_BEGIN = datetime(int(YEAR), 12, 29, 0, 0, 0, 0, timezone.utc)
                    DATE_TIME_END = datetime(int(YEAR) + 1, 1, 1, 0, 0, 0, 0, timezone.utc)
                else:
                    DATE_TIME_BEGIN = datetime(int(YEAR), int(month), 29, 0, 0, 0, 0, timezone.utc)
                    DATE_TIME_END = datetime(int(YEAR), int(month) + 1, 1, 0, 0, 0, 0, timezone.utc)
    
                proc = Process(target=download_tracks_week, args=(month, 5, DATE_TIME_BEGIN, DATE_TIME_END,))
                procs.append(proc)
                proc.start()
    
    # complete the processes
        for proc in procs:
            proc.join()

print((time.time()-start_time)/60)
