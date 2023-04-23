
AIRPORT_ICAO = "ESSA"
#AIRPORT_ICAO = "ESGG"
#AIRPORT_ICAO = "EIDW" # Dublin
#AIRPORT_ICAO = "LOWW" # Vienna

#AIRPORT_ICAO = "ESNQ" # Kiruna, no flights
#AIRPORT_ICAO = "ESNN" #Sundsvall, no flights
#AIRPORT_ICAO = "ESNO" #Ovik, no flights
#AIRPORT_ICAO = "ESNU" #Umeo
#AIRPORT_ICAO = "ESMS" #Malmo
#AIRPORT_ICAO = "ESSL" #Linkoping 

#AIRPORT_ICAO = "ENGM" # Oslo

#DEPARTURE = True
DEPARTURE = False

YEAR = '2019'

#MONTHS = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
MONTHS = ['10']

#WEEKS = [1,2,3,4,5]
WEEKS = [5]


# AREA possible values: "TMA", "CIRCLE"
#AREA = "TMA"
AREA = "CIRCLE"

RADIUS = 50 # in NM

# append the path of the parent directory
import sys
sys.path.append("..")

if AIRPORT_ICAO == "ESSA":
    from airports.constants_ESSA import *
elif AIRPORT_ICAO == "ESGG":
    from airports.constants_ESGG import *
elif AIRPORT_ICAO == "EIDW":
    from airports.constants_EIDW import *
elif AIRPORT_ICAO == "LOWW":
    from airports.constants_LOWW import *
elif AIRPORT_ICAO == "ESSL":
    from airports.constants_ESSL import *
elif AIRPORT_ICAO == "ENGM":
    from airports.constants_ENGM import *

# center of the circle
if AIRPORT_ICAO == "ESSA":
    # center of runway 01R
    CENTRAL_LAT = 59.64
    CENTRAL_LON = 17.95
elif AIRPORT_ICAO == "ESGG":
    # center of TMA
    CENTRAL_LAT = 58.097
    CENTRAL_LON = 12.444
elif AIRPORT_ICAO == "EIDW":
    # center of runway 28L
    CENTRAL_LAT = 53.42
    CENTRAL_LON = -6.27
elif AIRPORT_ICAO == "LOWW":
    # center of runway 16
    CENTRAL_LAT = 48.11
    CENTRAL_LON = 16.59
else:
    CENTRAL_LAT = sum(TMA_lat)/len(TMA_lat)
    CENTRAL_LON = sum(TMA_lon)/len(TMA_lon)

# append the path of the parent directory
import sys
sys.path.append("..")

if AIRPORT_ICAO == "ESSA":
    from airports.constants_ESSA import *
elif AIRPORT_ICAO == "ESGG":
    from airports.constants_ESGG import *
elif AIRPORT_ICAO == "EIDW":
    from airports.constants_EIDW import *
elif AIRPORT_ICAO == "LOWW":
    from airports.constants_LOWW import *
elif AIRPORT_ICAO == "ESSL":
    from airports.constants_ESSL import *
elif AIRPORT_ICAO == "ENGM":
    from airports.constants_ENGM import *