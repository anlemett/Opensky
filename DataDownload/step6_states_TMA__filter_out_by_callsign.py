from config import *

import os

DATA_DIR = os.path.join("..", "Data")
DATA_DIR = os.path.join(DATA_DIR, AIRPORT_ICAO)
DATA_DIR = os.path.join(DATA_DIR, YEAR)

area = (str(RADIUS) + "NM", "TMA")[AREA == "TMA"]
INPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + '_' + YEAR + "_extracted")
OUTPUT_DIR = os.path.join(DATA_DIR, "osn_" + AIRPORT_ICAO + "_states_" + area + '_' + YEAR + "_filtered")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

import pandas as pd
import calendar

import time
start_time = time.time()

flight_type = "Departure" if DEPARTURE else "Arrival"


def getCallsign(flight_id):
    
    return flight_id[6:]


for month in MONTHS:
    
    for week in WEEKS:
        
        if week == 5 and month == '02' and not calendar.isleap(int(YEAR)):
            continue
        
        print("STEP6", flight_type, area, AIRPORT_ICAO, YEAR, month, week)
        
        filename = AIRPORT_ICAO + '_states_' + area + '_extracted_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
        
        if DEPARTURE:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_arrival_' + filename
        
        full_filename = os.path.join(INPUT_DIR, filename)
        print(full_filename)
        
        df = pd.read_csv(full_filename, sep=' ',
            names = ['flightId', 'sequence', 'timestamp', 'lat', 'lon', 'altitude', 'velocity', 'beginDate', 'endDate'],
            index_col=False,
            dtype=str)
        
        
        # Remove the following callsigns
        
        #######################################################################
        # Consists of only letters
        # For Sweden: swedish police helicopters (SEMIX, SEXTD, SEJP, SEJPN, SEJPX, SEJPO, ...)
        #######################################################################
        
        df['callsign'] = df.apply(lambda row: getCallsign(row['flightId']), axis=1)
        df = df[~df["callsign"].str.isalpha()]
        
        
        #######################################################################
        # Consists of only digits
        # For Sweden: Scandinavian Air Ambulance
        #######################################################################
        
        df = df[~df["callsign"].str.isdigit()]
        
        #######################################################################
        # Short callsign (<=3)
        # E.g.: C33 - Austrian air rescue helicopter, Christophorus 3 - Air Medical Services
        # Usually commercial callsigns' length is 6 or 7, sometimes - 5 or 4
        #######################################################################
        
        #print(df.head(1))
        df['callsign_len'] = df["callsign"].apply(len)
        df = df[df["callsign_len"]>3]
        df = df.drop('callsign_len', 1)
        
        
        # Sweden specific
        
        #######################################################################
        # Starting with DFL ((Babcock Scandinavian Air Ambulance)
        # (contains 'DFL' also works)
        #######################################################################
        
        searchfor = ['DFL']
        
        df = df[~df.flightId.str.contains('|'.join(searchfor))]
        
        #######################################################################
        # Starting with SVF (Swedish Armed Forces )
        # (contains 'SVF' also works)
        #######################################################################
        
        searchfor = ['SVF']
        
        df = df[~df.flightId.str.contains('|'.join(searchfor))]
        
        #######################################################################
        # Starting with HMF (Swedish Maritime Administration )
        # (contains 'HMF' also works)
        #######################################################################
        
        searchfor = ['HMF']
        
        df = df[~df.flightId.str.contains('|'.join(searchfor))]
        
        # TODO: maybe cargo airlines? E.g. starting with FDX?
        
        
        df = df.drop('callsign', 1)
        
        filename = AIRPORT_ICAO + '_states_' + area +'_filtered_' + YEAR + '_' + month + '_week' + str(week) + '.csv'
        
        if DEPARTURE:
            filename = 'osn_departure_' + filename
        else:
            filename = 'osn_arrival_' + filename

        
        full_filename = os.path.join(OUTPUT_DIR, filename)
        
        df.to_csv(full_filename, sep=' ', encoding='utf-8', float_format='%.6f', header=False, index=False)

print((time.time()-start_time)/60)
