
AIRPORT_ICAO = "ESSA"
#AIRPORT_ICAO = "ESGG"
#AIRPORT_ICAO = "EIDW" # Dublin
#AIRPORT_ICAO = "LOWW" # Vienna

#AIRPORT_ICAO = "ESNQ" # Kiruna, no flights
#AIRPORT_ICAO = "ESNN" #Sundsvall, no flights
#AIRPORT_ICAO = "ESNO" #Ovik, no flights
#AIRPORT_ICAO = "ESNU" #Umeo
#AIRPORT_ICAO = "ESMS" #Malmo

#DEPARTURE = True
DEPARTURE = False

YEAR = '2020'

#MONTHS = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
MONTHS = ['02']

#WEEKS = [1,2,3,4,5]
WEEKS = [5]


# AREA possible values: "TMA", "CIRCLE"
AREA = "TMA"
#AREA = "CIRCLE"

RADIUS = 50 # in NM

if AIRPORT_ICAO == "ESSA":
    # center of runway 01R
    CENTRAL_LAT = 59.64
    CENTRAL_LON = 17.95
elif AIRPORT_ICAO == "ESGG":
    # center of TMA
    CENTRAL_LAT = 58.097
    CENTRAL_LON = 12.444




