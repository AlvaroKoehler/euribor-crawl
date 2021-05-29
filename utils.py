from datetime import datetime
from pandas.tseries.offsets import BDay
import json 

EU_TIME_FORMAT = "%d/%m/%Y"
EU_EXT_TIME_FORMAT = "%d/%m/%Y %I:%M:%S"

def get_timestamp(format=EU_EXT_TIME_FORMAT):
    return datetime.today().strftime(format)

def from_pd_to_jsons(df):
    temp = df.reset_index(drop=True)
    analyze = temp.to_json(orient="index", date_format = "iso")
    parsed = json.loads(analyze)
    list_of_dics = [value for value in parsed.values()]
    return list_of_dics

def last_business_euribor_day(offset=1, to_str=False, time_fmt=EU_TIME_FORMAT):
    today = datetime.today()
    # If is satudary or sunday we have to change the offset 
    if today.weekday() == 5:
        offset = 1
    elif today.weekday() == 6:
        offset = 2
    last_Bday = today - BDay(offset)
    if to_str:
        return last_Bday.strftime(time_fmt)
    else:
        return last_Bday