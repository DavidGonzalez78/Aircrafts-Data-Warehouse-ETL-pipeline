from tqdm import tqdm
import logging
from pygrametl.datasources import CSVSource, SQLSource
from datetime import datetime
from typing import Any, TypeAlias



# Configure logging
logging.basicConfig(
    filename='cleaning.log',           # Log file name
    level=logging.INFO,           # Logging level
    format='%(message)s'  # Log message format
)


# region MANAGE DATETIMES

def build_dateCode(date:datetime) -> str:
    return f"{date.year}-{date.month}-{date.day}"


def build_monthCode(date:datetime) -> str:
    return f"{date.year}{str(date.month).zfill(2)}"


def build_day_dimension_value(date:datetime) -> tuple[str, int, str]: #day_id, day, month_id
    '''Returns a tuple corresponding to a row of table days'''
    return ( build_dateCode(date), date.day, build_monthCode(date) )


def build_month_dimension_value(date:datetime) -> tuple[str, int, int]: #month_id, month, year
    '''Returns a tuple corresponding to a row of table months'''
    return ( build_monthCode(date), date.month, date.year )


def time_difference( start:datetime, end:datetime ) -> int:
    '''Recieves two datetimes and returns their difference in seconds'''
    try:
        diff = end - start
        return diff.total_seconds()
    except: 
        return None


def hour_code(date:datetime) -> str:
    '''Recieves a datetime and returns only it's hour code, like 16:30:02 for example'''
    try: return date.strftime('%H:%M:%S')
    except: return "[invalid date]"

# endregion



# region MANAGE OVERLAPPINGS (BR-21)
Slot: TypeAlias = tuple[float, float] #Los dos elementos son la hora del día. Mes y año no hace falta porque esto se guarda a nivel de día
def overlap(s1:Slot, s2:Slot):
    '''Returns whether both intervals overlap'''
    return s1[0]<s2[1] and s2[0]<s1[1]


def overlaps_with_dict(s:Slot, l:list[Slot]):
    '''Returns true if interval s overlaps with any of the intervals on list of intervals l'''
    return any( [ overlap(s, si) for si in l ] )

# endregion



# region MANAGE CSV FILES
def fill_aircrafts( table_aircrafts: dict[str, dict], aircrafts_csv_source:CSVSource ):
    '''Recieves CSVSource of the aircrafts and adds them into their table dictionary'''

    for row in aircrafts_csv_source:
        registration, model, manufacturer = row['registration'], row['model'], row['manufacturer']
        table_aircrafts[registration] = { 'model': model, 'manufacturer': manufacturer }


def fill_reporteurs( table_reporteurs:dict[str, dict], personnel_csv_source:CSVSource  ):
    '''Recieves CSVSource of the reporteurs and adds them into their table dictionary'''

    for row in personnel_csv_source:
        reporteurid, airport = str(row['reporteurid']).strip(), row['airport']
        table_reporteurs[reporteurid] = { 'airport': airport, 'role': None }

# endregion



def void_monthly_metrics() -> dict[str, Any]:
    '''Returns the default starting value for an element of table_monthly_usage'''
    return {'dy': 0, 'cn': 0, 'dh': 0, 'ados': 0, 'adoss': 0, 'adosu': 0, 'adis': 365.25/12}





### -------------------------------------------------------------------------------------------------- ###
def transform_flights(      source_flights:SQLSource, 
                            table_daily_usage:dict[tuple[str,str], dict], 
                            table_monthly_usage:dict[tuple[str, str], dict], 
                            table_days:set[tuple[str, int, str]], 
                            table_months:set[tuple[str, int, int]], 
                            br21_slots: dict[ tuple[str, str], list[Slot] ], 
                            apply_business_rules:bool = True
                            ):
    
    '''Traverses all flights extracted from AMOS.flights and saves their information into the usage metrics tables'''

    swapped_flights = 0

    #Loop that traverses all flights
    for i, flight in tqdm( enumerate(source_flights), total=69095, desc="Flights    "):

        # Get aircraft and date
        aircraft:str = flight['aircraftregistration']
        date:datetime = flight['scheduleddeparture']
        day:str = build_dateCode(date)
        month:str = build_monthCode(date)
        monthly_key = (aircraft, month)
        daily_key = (aircraft, day)

        #Add date into the months and days table
        table_days.add( build_day_dimension_value(date) )
        table_months.add( build_month_dimension_value(date) )

        #Raw flight variables
        actual_arrival:datetime = flight['actualarrival']
        actual_departure:datetime = flight['actualdeparture']
        scheduled_arrival:datetime = flight['scheduledarrival']
        scheduled_departure:datetime = flight['scheduleddeparture']
        cancelled:bool = flight['cancelled'] or actual_arrival is None or actual_departure is None 

        if daily_key not in table_daily_usage: 
            table_daily_usage[daily_key] = { 'fh': 0, 'tos': 0, 'sto': 0 }
        
        if monthly_key not in table_monthly_usage: 
            table_monthly_usage[monthly_key] = void_monthly_metrics()

        #Complex flight variables
        if cancelled: 
            table_monthly_usage[monthly_key]['cn'] += 1
            table_daily_usage[daily_key]['sto'] += 1
        
        else:

            #Slot overlapping
            ignore:bool = False
            if apply_business_rules:
                slot = (actual_departure.hour, actual_arrival.hour)
                if daily_key not in br21_slots: br21_slots[daily_key] = []
                ignore = overlaps_with_dict( slot, br21_slots[daily_key] )
                
                if ignore: 
                    logging.error( f"BR-21: Flight of aircraft {aircraft} at time {slot} overlaps with an existing slot! That day there were those other slots: {br21_slots[daily_key]}")
                else: 
                    br21_slots[daily_key].append(slot)

            if not ignore: 
                this_flight_hours:float = time_difference(actual_departure, actual_arrival) / 3600
                
                #BR-23
                if apply_business_rules and this_flight_hours < 0: 
                    actual_arrival, actual_departure = actual_departure, actual_arrival
                    this_flight_hours:float = time_difference(actual_departure, actual_arrival) / 3600
                    swapped_flights+=1
                    logging.error( f"BR-23: Flight of aircraft {aircraft} at {build_day_dimension_value(date)} had departure and arrival swapped" )
                
                this_delay_hours:float = time_difference(scheduled_departure, actual_departure) / 3600
                delayed:bool = (this_delay_hours > 15/60) #El profe dijo que ignorasemos lo de <6h
                if not delayed: this_delay_hours = 0

            
                # Add the computations to the metrics tables
                table_daily_usage[daily_key]['fh'] += this_flight_hours
                table_daily_usage[daily_key]['tos'] += 1
                table_daily_usage[daily_key]['sto'] += 1

                table_monthly_usage[monthly_key]['dh'] += this_delay_hours
                table_monthly_usage[monthly_key]['dy'] += delayed
    
    if apply_business_rules: logging.info( f"\n\nBR-23: There were {swapped_flights}/69095 that had arrival and departure times swapped!" )





### -------------------------------------------------------------------------------------------------- ###
def transform_maintenances(     source_maintenances:SQLSource, 
                                table_monthly_usage:dict[tuple[str, str], dict ], 
                                table_months: set[tuple[str, int, int]],
                                br21_slots: dict[ tuple[str, str], list[Slot] ],
                                apply_business_rules:bool = True
                                ):

    '''Traverses all maintenances extracted from AMOS.maintenances and saves their information into the usage metrics tables'''

    #Traverse maintenances
    for i, maintenance in tqdm( enumerate(source_maintenances), total=148524, desc="Maintenance"):

        # Get aircraft and date
        aircraft:str = maintenance['aircraftregistration']
        date:datetime = maintenance['scheduleddeparture']
        month:str = build_monthCode(date)
        monthly_key = (aircraft, month)

        # Raw maintenance variables
        scheduled_arrival:datetime = maintenance['scheduledarrival']
        scheduled_departure:datetime = maintenance['scheduleddeparture']
        scheduled:bool = maintenance['programmed']

        #Add date into the months table
        table_months.add( build_month_dimension_value(date) )

        #Overlapping
        ignore = False
        if apply_business_rules:
            daily_key = (aircraft, build_dateCode(date))
            slot = (scheduled_departure.hour, scheduled_arrival.hour)
            if daily_key not in br21_slots: br21_slots[daily_key] = []

            ignore =  overlaps_with_dict( slot, br21_slots[daily_key] )
            
            if ignore: logging.error( f"BR-21: Maintenance of aircraft {aircraft} at time {slot} overlaps with an existing slot! That day there were those other slots: {br21_slots[daily_key]}")
            else: br21_slots[daily_key].append(slot)
        
        if not ignore: 
            #Complex maintenance variables
            maintenance_time = time_difference(scheduled_departure, scheduled_arrival) / (3600*24) #Expresado en días
            scheduled_maintenance_time = maintenance_time if scheduled else 0
            unscheduled_maintenance_time = maintenance_time if not scheduled else 0

            # Add the computations to the metrics tables
            if not monthly_key in table_monthly_usage:
                table_monthly_usage[monthly_key] = void_monthly_metrics()

            table_monthly_usage[monthly_key]['ados'] += maintenance_time
            table_monthly_usage[monthly_key]['adoss'] += scheduled_maintenance_time
            table_monthly_usage[monthly_key]['adosu'] += unscheduled_maintenance_time
            table_monthly_usage[monthly_key]['adis'] -= maintenance_time






### -------------------------------------------------------------------------------------------------- ###
def transform_reports(      source_reports:SQLSource, 
                            table_aircrafts:dict[str, dict], 
                            table_reportage_usage: dict[ tuple[str, str, str], dict ], 
                            table_reporteurs:dict[str, dict[str, Any]], 
                            table_months:set[tuple[str, int, int]], 
                            apply_business_rules:bool = True
                            ):

    '''Traverses all reports extracted from AIMS.postflightreports and saves their information into the usage metrics tables'''

    reports_list = list(source_reports)
    foreign_aircraft_reports_count = 0 #Reports made on aircrafts that were not in our database

    #Loop that traverses all reports
    for i, report in tqdm( enumerate(reports_list), total=180418, desc="Reports    "):
    
        # Get aircraft and check if it is in our database
        aircraft:str = report['aircraftregistration']
        if aircraft in table_aircrafts:
            
            # Get other variables
            date = report['reportingdate']
            month:str = build_monthCode(date)
            reporteurid = str(report['reporteurid'])
            reporteur_class = report['reporteurclass']
            key = (aircraft, month, reporteurid)

            #Add date into the months table
            table_months.add( build_month_dimension_value(date) )


            # Not all reporteurs are in the csv file. Maybe we found a new one. Also the csv doesn't tell its role
            if reporteurid in table_reporteurs:
                table_reporteurs[reporteurid]['role'] = reporteur_class
            else: 
                table_reporteurs[reporteurid] = { 'airport': None, 'role': reporteur_class }


            # Add the computations to the metrics tables
            if not key in table_reportage_usage:
                table_reportage_usage[key] = { 'reps': 0, 'mareps': 0, 'pireps': 0 }

            table_reportage_usage[key]['reps'] += 1
            if reporteur_class == 'MAREP': table_reportage_usage[key]['mareps'] += 1
            elif reporteur_class == 'PIREP': table_reportage_usage[key]['pireps'] += 1
        

        else: #Business Rule
            logging.info( f"Reportage BR: Had a report on aircraft {aircraft} but that aircraft isn't in our database" )
            foreign_aircraft_reports_count += 1
    
    if apply_business_rules: logging.info( f"\n\nReportage BR: There were {foreign_aircraft_reports_count}/180418 reports with aircrafts that were not in our database\n\n" )





### -------------------------------------------------------------------------------------------------- ###
def transform( sources_extract:dict[str, CSVSource|SQLSource], apply_business_rules:bool = True) -> dict[str, list[dict]]:

    print("\n\n  --- Starting transform... ---  ")
    
    # Those dictionaries/sets contain the values to be added into the database
    table_days: set[tuple[str, int, str]] = set()                       # Each one is a row (day_id, day, month_id)
    table_months: set[tuple[str, int, int]] = set()                     # Each one is a row (month_id, month, year)
    table_reporteurs: dict[str, dict] = {}                              # La clave es el reporteur_id, el valor es un diccionario con airport y role
    table_aircrafts: dict[str, dict] = {}                               # La clave es el registration, el valor es un diccionario con el modelo y el manufacturer

    table_daily_usage: dict[ tuple[str, str], dict ] = {}               # La clave es la combinación (aircraft, day), el valor es un diccionario con las métricas 
    table_monthly_usage: dict[ tuple[str, str], dict ] = {}             # La clave es la combinación (aircraft, month), el valor es un diccionario con las métricas 
    table_reportage_usage: dict[ tuple[str, str, str], dict ] = {}      # La clave es la combinación (aircraft, day, reporteur_uid), el valor es un diccionario con las métricas 
    br21_slots: dict[ tuple[str, str], list[Slot] ] = {}                # Sirve para comprobar que no haya dos slots superpuestos (que un avión hiciese dos cosas a la vez), es de la BR-21

    
    #Fill the tables with the processed data from the extraction
    fill_aircrafts(table_aircrafts, sources_extract['aircraft-manufacturer-info']) # type: ignore
    fill_reporteurs(table_reporteurs, sources_extract['maintenance-personnel']) # type: ignore

    transform_flights(sources_extract['AIMS.flights'], table_daily_usage, table_monthly_usage, table_days, table_months, br21_slots, apply_business_rules)
    transform_maintenances(sources_extract['AIMS.maintenance'], table_monthly_usage, table_months, br21_slots, apply_business_rules)
    transform_reports(sources_extract['AMOS.postflightreports'], table_aircrafts, table_reportage_usage, table_reporteurs, table_months, apply_business_rules)


    #Turn the dictionaries into lists. Each element of the lists is a row ready to be inserted into the data warehouse
    transform_sources = {}

    transform_sources['days'] =   [    {'day_id': row[0], 'day': row[1], 'month_id': row[2]} for row in table_days    ]
    transform_sources['months'] = [    {'month_id': row[0], 'month': row[1], 'year': row[2]} for row in table_months    ]
    transform_sources['aircrafts'] =        [ {'registration': key} | value        for key, value in table_aircrafts.items() ]
    transform_sources['reporteurs'] =       [ {'reporteur_uid': key} | value      for key, value in table_reporteurs.items() ]
    
    transform_sources['daily_usage'] =      [ {'registration': key1, 'day_id': key2} | value                               for (key1, key2),       value in table_daily_usage.items() ]
    transform_sources['monthly_usage'] =    [ {'registration': key1, 'month_id': key2} | value                             for (key1, key2),       value in table_monthly_usage.items() ]
    transform_sources['reportage_usage'] =  [ {'registration': key1, 'month_id': key2, 'reporteur_uid': key3} | value      for (key1, key2, key3), value in table_reportage_usage.items() ]


    print("  --- Transform finished ---  ")
    return transform_sources

