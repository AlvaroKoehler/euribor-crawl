from utils import EU_TIME_FORMAT, last_business_euribor_day
import pandas as pd
import datetime


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
            
            '''
                Do we need a DF filtered by a particular time dimension** ?
                    **time dimension: 1w, 1m, 3m, 6m, 12m

                The df has got a date as index
                I'm about to reset it and rename as date with the EU Format

                Then I return it
            '''
            cols = ['1w', '1m', '3m', '6m', '12m']
            if q_filter and q_filter in df_built.columns:
                cols = q_filter
            df = df_built[cols].reset_index()
            df.rename(columns={'index': 'date'}, inplace=True)
            df['date'] = pd.to_datetime(
                df['date'], 
                dayfirst=True,
                format=EU_TIME_FORMAT
                )
            # Appending 'eur_' to each column
            df.columns = ['eur_'+col for col in df.columns]
            return df
        except Exception as e:
            raise(e)
 
    def build_history(self,from_date=2011):
        # Create an empty data frame 
        df_hist = pd.DataFrame()
        # For every year, create data set and append it to the empty data farme
        for year in range(from_date, self.current_year + 1):
            df_temp = self.get_data_from_year(year)
            df_hist = df_hist.append(df_temp)
        df_hist['eur_year'] = df_hist.eur_date.dt.year
        df_hist['eur_month'] = df_hist.eur_date.dt.month
        return df_hist

    def get_last_euribor_rate_dict(self):
        last_bday = last_business_euribor_day(to_str=True)
        url = self._build_url()
        csv = pd.read_csv(url)
        try:
            assert(last_bday in csv.columns)
        except AssertionError:
            raise AssertionError(f'{last_bday} not in the index')

        last_items = csv[last_bday]
        eur_date = pd.to_datetime(last_bday)
        dict_euribor={
            'eur_date': eur_date.strftime(EU_TIME_FORMAT),
            'eur_year': eur_date.year,
            'eur_month': eur_date.month,
            'eur_1w': last_items[0],
            'eur_1m': last_items[1],
            'eur_3m': last_items[2],
            'eur_6m': last_items[3],
            'eur_12m': last_items[4]
        }
        return dict_euribor
