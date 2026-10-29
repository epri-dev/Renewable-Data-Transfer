
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import time as time
from dateutil.relativedelta import relativedelta
import pytz
from dateutil import parser
# import pysftp

# PIconnect stuff (try to connect twice, seems to fix SocketException error)
try:
    import PIconnect as PI
    from PIconnect.PIConsts import CalculationBasis, SummaryType, TimestampCalculation
except:
    import PIconnect as PI
    from PIconnect.PIConsts import CalculationBasis, SummaryType,TimestampCalculation

server_name = 'PISERVER'
PI.PIConfig.DEFAULT_TIMEZONE = 'UTC' # utc recommended
timezone = pytz.timezone('UTC')
interval = '60m' # PI data averaging interval
start_time = '2026-03-10 14:00'

# increment time for end or query
rd = relativedelta(hours=+48) # could use: months, weeks, days, hours

plant_prefixes = ['abc','def']


     
# reformat timestamps
time_format = '%Y-%m-%d %H:%M:%S%z'
start = timezone.localize(parser.parse(start_time)).strftime(time_format)
end = timezone.localize(parser.parse(start_time) + rd).strftime(time_format)

for plant_prefix in plant_prefixes:
    with PI.PIServer(server=server_name) as server:
        points = server.search(plant_prefix + '*')

        # !!!!!!!!!!!!!!!!
        # points = points[0:100] 
        # !!!!!!!!!!!!!!!!
        
        try: 
            temp = [point.interpolated_values(start, end, interval) for point in points]
        except:
            print('pausing due to an error')
            time.sleep(5) # 5 = pause for 5 seconds
            temp = [point.interpolated_values(start, end, interval) for point in points]
    
    df = pd.concat(temp, axis = 1)
    del temp
    df = df.T
    names = [point.name for point in points]
    uom = [point.units_of_measurement for point in points]
    descriptions = [point.description for point in points]
    df.insert(0,'name',names)
    df.insert(1,'uom',uom)
    df.insert(2,'description',descriptions)
    df = df.set_index('name')

    path_name = r'path\to\save\csv\files'
    file_name = plant_prefix + '_tag_list_rev4_not_numeric.csv'
    full_path = (path_name + "\\" + file_name)

    df.to_csv(full_path)
