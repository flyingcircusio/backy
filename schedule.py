import time
import datetime

now = time.time()
days = 24*60*60

tag_none_marker = object()



schedule = {
    '': {'interval': 0.5*days,
         'keep': 3*days},
    'bi-weekly': {'interval': 3.5*days,
                  'keep': 14*days,
                  'type': '+'},
    'monthly': {'interval': 30*days,
                'keep': 180*days,
                'type': 'F'}}

schedule = {
    '': {'interval': 1*days,
         'keep': 8*days},
    'weekly': {'interval': 7*days,
               'keep': 5*7*days,
               'type': '+'},
    'monthly': {'interval': 30*days,
                'keep': 90*days,
                'type': 'F'}}


