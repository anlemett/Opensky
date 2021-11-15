from config import *

import os

DATA_DIR = os.path.join("..", "Data")
DATA_DIR = os.path.join(DATA_DIR, AIRPORT_ICAO)
DATA_DIR = os.path.join(DATA_DIR, YEAR)

area = ("around" + str(RADIUS) + "NM", "aroundTMA")[AREA == "TMA"]
INPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_tracks_" + area + "_" + YEAR)
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + "_" + YEAR)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


from datetime import datetime
import pytz

from opensky_credentials import USERNAME, PASSWORD

import paramiko
from io import StringIO
import re

import numpy as np
import pandas as pd
import calendar

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

flight_type = "Departure" if DEPARTURE else "Arrival"


def get_df(impala_log, time_begin, time_end):

    s = StringIO()
    count = 0
    for line in impala_log.readlines():
        line = line.strip()
        if re.match("\|.*\|", line):
            count += 1
            s.write(re.sub(" *\| *", ",", line)[1:-2])
            s.write("\n")

    #contents = s.getvalue()
    #print(contents)
    
    if count > 0:
        s.seek(0)
        df = pd.read_csv(s, sep=',', error_bad_lines=False, warn_bad_lines=True)
        df = df.fillna(0)
        df.index = df.index.set_names(['sequence'])
        df.columns = ['timestamp', 'lat', 'lon', 'altitude', 'velocity']
        df[['lat', 'lon']] = df[['lat', 'lon']].apply(pd.to_numeric, downcast='float', errors='coerce').fillna(0)
        df[['altitude', 'velocity']] = df[['altitude', 'velocity']].apply(pd.to_numeric, downcast='integer', errors='coerce').fillna(0)
        df['altitude'] = df['altitude'].astype(int)
        df['velocity'] = df['velocity'].astype(int)
        
        begin_datetime = datetime.utcfromtimestamp(time_begin)
        df['beginDate'] = begin_datetime.strftime('%y%m%d')

        end_datetime = datetime.utcfromtimestamp(time_end)
        df['endDate'] = end_datetime.strftime('%y%m%d')

        df.reset_index(level=df.index.names, inplace=True)
        return df

def connectToImpala():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    while True:
        try:
            print("trying to connect")
            client.connect(
                hostname = 'data.opensky-network.org',
                port=2230,
                username=USERNAME,
                password=PASSWORD#,
                #timeout=120,
                #look_for_keys=False,
                #allow_agent=False,
                #compress=True
                )
            break
        except paramiko.SSHException as err:
            #print(err)
            time.sleep(2)
        except:
            print("exception")
            time.sleep(2)

    return client


def getShellReady(shell):
    while not shell.recv_ready():
        time.sleep(1)

    total = ""
    while len(total) == 0 or total[-10:] != ":21000] > ":
        b = shell.recv(256)
        total += b.decode()


def closeConnection(client, shell):
    shell.close()
    client.close()


#Set in this function which fields to extract (in sql request)
def request_states(shell, icao24, time_begin, time_end):
    
    #print("request_states", icao24)

    time_begin_datetime = datetime.utcfromtimestamp(time_begin)
    time_begin_datetime = time_begin_datetime.replace(tzinfo=pytz.timezone('UTC'))
    hour_begin_datetime = time_begin_datetime.replace(microsecond=0,second=0,minute=0)
    hour_begin_timestamp = int(datetime.timestamp(hour_begin_datetime))
    
    time_end_datetime = datetime.utcfromtimestamp(time_end)
    time_end_datetime = time_end_datetime.replace(tzinfo=pytz.timezone('UTC'))
    hour_end_datetime = time_end_datetime.replace(microsecond=0,second=0,minute=0)
    hour_end_timestamp = int(datetime.timestamp(hour_end_datetime))

    time_begin_str = str(time_begin)
    time_end_str = str(time_end)
    hour_begin_str = str(hour_begin_timestamp)
    hour_end_str = str(hour_end_timestamp)


    request = "select time, lat, lon, baroaltitude, velocity from state_vectors_data4 where icao24=\'" + icao24 + "\' and time>=" + time_begin_str + " and time<=" + time_end_str + " and hour>=" + hour_begin_str + " and hour<=" + hour_end_str + ";\n"
    
    while not shell.send_ready():
        time.sleep(1)

    shell.send(request)


    while not shell.recv_ready():
        time.sleep(1)
    
    total = ""
    count = 0
    while len(total) == 0 or total[-10:] != ":21000] > ":
        count = count + 1
        b = shell.recv(256)
        total += b.decode()

    impala_log = StringIO(total)
    
    return get_df(impala_log, time_begin, time_end)


def download_states_week(month, week):
    
    client = connectToImpala()

    shell = client.invoke_shell()
    
    getShellReady(shell)
    
    # opensky tracks csv
    opensky_tracks_filename = AIRPORT_ICAO + '_tracks_' + area + '_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
    if DEPARTURE:
        opensky_tracks_filename = 'osn_departure_' + opensky_tracks_filename
    else:
        opensky_tracks_filename = 'osn_arrival_' + opensky_tracks_filename

    #opensky states close to TMA csv
    opensky_states_filename = AIRPORT_ICAO + '_states_'+ area + '_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
    if DEPARTURE:
        opensky_states_filename = 'osn_departure_' + opensky_states_filename
    else:
        opensky_states_filename = 'osn_arrival_' + opensky_states_filename

    opensky_states_df = pd.DataFrame()

    if DEPARTURE:
        opensky_tracks_df = pd.read_csv(os.path.join(INPUT_DIR, opensky_tracks_filename), sep=' ',
            names=['flightId', 'sequence', 'destination', 'beginDate', 'callsign', 'icao24', 'timestamp', 'lat', 'lon', 'baroAltitude'],
            index_col=[0,1], dtype={'flightId':str, 'sequence':int, 'icao24': str, 'timestamp':int})
    else:
        opensky_tracks_df = pd.read_csv(os.path.join(INPUT_DIR, opensky_tracks_filename), sep=' ',
            names=['flightId', 'sequence', 'origin', 'endDate', 'callsign', 'icao24', 'timestamp', 'lat', 'lon', 'baroAltitude'],
            index_col=[0,1], dtype={'flightId':str, 'sequence':int, 'icao24': str, 'timestamp':int})



    number_of_flights = len(opensky_tracks_df.groupby(level='flightId'))

    count = 0
    
    for flight_id, new_df in opensky_tracks_df.groupby(level='flightId'):
        count = count + 1
        
        print("STEP3 Downloading", flight_type, area, AIRPORT_ICAO, YEAR, month, week, number_of_flights, count)
        
        (id, first_index) = new_df.index[0]
        (id, last_index) = new_df.index[-1]
        
        icao24 = new_df.loc[(flight_id, first_index)]['icao24']
        time_begin = new_df.loc[(flight_id, first_index)]['timestamp']
        time_end = new_df.loc[(flight_id, last_index)]['timestamp']
        
        flight_id_states_df = request_states(shell, icao24, time_begin, time_end)
        
        if flight_id_states_df is not None and not flight_id_states_df.empty:
            
            flight_id_states_df.insert(0, 'flight_id', flight_id)
            
            flight_id_states_df.set_index(['flight_id'], inplace=True)
            
            opensky_states_df = pd.concat([opensky_states_df, flight_id_states_df], axis=0, sort=False)
    
    # fix "time" inserted
    opensky_states_df = opensky_states_df[opensky_states_df.timestamp != "time"]
    opensky_states_df = opensky_states_df.astype({"timestamp": int})
    #opensky_states_df["timestamp"] = pd.to_numeric(opensky_states_df["timestamp"])
    
    # sort timestamps and reassign sequence
    
    number_of_flights = len(opensky_states_df.groupby(level='flight_id'))
    count = 0
    
    new_states_df = pd.DataFrame()
    
    for flight_id, flight_id_group in opensky_states_df.groupby(level='flight_id'):
        
        count = count + 1
        print("STEP3 Fixing", flight_type, area, AIRPORT_ICAO, YEAR, month, week, number_of_flights, count)
        
        flight_id_group_length = len(flight_id_group)
        
        sequence_list = list(range(flight_id_group_length))
        
        new_flight_df = flight_id_group.sort_values(by=['timestamp'])
        
        new_flight_df = new_flight_df.drop(['sequence'], axis=1)
        
        new_flight_df['sequence'] = sequence_list

        new_flight_df = new_flight_df[['sequence', 'timestamp', 'lat', 'lon', 'altitude', 'velocity', 'beginDate', 'endDate']]
        
        new_states_df = new_states_df.append(new_flight_df)
        
    new_states_df.to_csv(os.path.join(OUTPUT_DIR, opensky_states_filename), sep=' ', encoding='utf-8', float_format='%.6f', header=None, index = True)
    
    closeConnection(client, shell)


import time
start_time = time.time()

from multiprocessing import Process

if __name__ == '__main__':
    for month in MONTHS:

        procs = [] 
    
        for week in WEEKS:
        
            if week == 5 and month == '02' and not calendar.isleap(int(YEAR)):
                continue
        
            proc = Process(target=download_states_week, args=(month, week))
            procs.append(proc)
            proc.start()
        
        # complete the processes
        for proc in procs:
            proc.join()

print((time.time()-start_time)/60)
