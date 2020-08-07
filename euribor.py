import pandas as pd
import datetime
import json 
from pandas.tseries.offsets import BDay

EU_TIME_FORMAT = "%d/%m/%Y"

def from_pd_to_jsons(df):
    temp = df.reset_index(drop=True)
    # temp['date'] = temp['date'].dt.strftime("%d/%m/%Y")
    analyze = temp.to_json(orient="index", date_format = "iso")
    parsed = json.loads(analyze)
    list_of_dics = [value for value in parsed.values()]
    return list_of_dics


def last_business_day(offset=1, to_str=False, time_fmt="%d/%m/%Y"):
    today = datetime.datetime.today()
    last_Bday = today - BDay(offset)
    if to_str:
        return last_Bday.strftime(time_fmt)
    else:
        return last_Bday

'''
    Websites to get information from:

    Euribor Daily Rates
    https://www.emmi-benchmarks.eu/euribor-org/euribor-rates.html

    Euribor Calculation 
    https://www.emmi-benchmarks.eu/assets/files/Euribor_tech_features.pdf

    Statistical Data Warehouse - European Central Bank 
    https://sdw.ecb.europa.eu/home.do;jsessionid=D0D170A9308EE0D95CFA78516F888A96

    Suomen Pankki - Interest Rates
    https://www.suomenpankki.fi/en/Statistics/interest-rates/charts/korot_kuviot/euriborkorot_pv_chrt_en/

    Inflation 
    https://www.ecb.europa.eu/mopo/html/index.en.html
    https://www.ecb.europa.eu/stats/macroeconomic_and_sectoral/hicp/html/index.en.html

    TeleTrader Dashboard
    https://www.teletrader.com/bonds/libormatrix?ts=1595950241389

'''
 
''' 
    Utils 
    https://www.expansion.com/blogs/conthe/2017/07/21/un-calculo-poco-armonico.html

'''

'''
    ToRead

    https://dominatuscuentas.wordpress.com/2016/02/05/euribor-el-bce-y-la-inflacion/
    https://www.euribor.com.es/2020/05/04/el-retorno-de-la-inflacion/
    https://www.rankia.com/blog/mejores-hipotecas/4413688-hipotecas-tipo-fijo-inflacion
    https://www.euribor-rates.eu/es/que-es-el-euribor/
    
'''
class EuriborCrawl:
    
    def __init__(self):
      self.current_year = datetime.date.today().year
    
    def _build_url(self, year=None):
        if not year:
            year = self.current_year
        return f"https://www.emmi-benchmarks.eu/assets/components/rateisblue/file_processing/publication/processed/hist_EURIBOR_{year}.csv"
  
    def get_data_from_year(self, year=None, q_filter=None):
        if not year:
            year = self.current_year
        
        try:
            # Set up and query the data
            print(f'Processing year: {year}')
            url = self._build_url(year)
            t = pd.read_csv(url)
            # Remove NaN
            t_clean = t.dropna(axis=0, how='all')
            t_clean = t_clean.dropna(axis=1, how='all')
            # Traspose 
            df = t_clean.T
 
            # Update columns
            df.columns = df.iloc[0]
            df = df.drop(df.index[0])
 
            # Build the clean DataFrame
            df_built = pd.DataFrame()
 
            # Add only numerical values (we have odd text in some CSVs)
            try:
                for col in df.columns:
                    df_built[col] = pd.to_numeric(df[col])
            except ValueError as ve:
                print(f'Value not transformed: {ve}')
        
            # Clean up
            del(df)
            del(t)
            del(t_clean)
            
            # Return the result, filtered by time dimension if needed  
            if q_filter and q_filter in df_built.columns:
                df = df_built[[q_filter]].reset_index()
                df.rename(columns={'index': 'date'}, inplace=True)
                df['date'] = pd.to_datetime(
                    df['date'], 
                    dayfirst=True, 
                    format=EU_TIME_FORMAT
                    )
                return df
            else:
                cols = ['1w', '1m', '3m', '6m', '12m']
                df = df_built[cols].reset_index()
                df.rename(columns={'index': 'date'}, inplace=True)
                df['date'] = pd.to_datetime(
                    df['date'], 
                    dayfirst=True,
                    format=EU_TIME_FORMAT
                    )
                return df
        except Exception as e:
            raise(e)
 
    def build_history(self,from_date=None):
        df_hist = pd.DataFrame()
 
        if not from_date:
            from_date = 2010
        
        for year in range(from_date, self.current_year + 1):
            df_temp = self.get_data_from_year(year)
            df_hist = df_hist.append(df_temp)
        return df_hist

    def get_last_euribor_rate(self):
        last_bday = last_business_day(to_str=True)
        url = self._build_url()
        csv = pd.read_csv(url)
        try:
            assert(last_bday in csv.columns)
        except AssertionError:
            raise AssertionError(f'{last_bday} not in the index')

        last_items = csv[last_bday]
        dict_euribor={
            'eur_date': pd.to_datetime(last_bday, format=EU_TIME_FORMAT),
            'eur_1w': last_items[0],
            'eur_1m': last_items[1],
            'eur_3m': last_items[2],
            'eur_6m': last_items[3],
            'eur_12m': last_items[4],
        }
        print(dict_euribor)
        return dict_euribor
