#from alpha_vantage.techindicators import TechIndicators
from alpha_vantage.timeseries import TimeSeries
from decouple import config
import pandas as pd

#pylint: disable=no-member
#pylint: disable=unbalanced-tuple-unpacking

#TODO Sector Performance
#TODO Tech Indicators
#TODO Find TWEXB
#TODO FOREX?
#TODO Other Macro?

class Wrangle:
    """
    Class based data wrangling function. Uses the APIs to 
    import the data, then wrangles it together.

    -----------------------------
    Parameters
    -----------------------------
    symbol : str
        string ticker used to identify the security. eg['AAPL', 'SPX'... etc.]
    interval : str
        time interval between two conscutive values,supported values 
        are: ['1min', '5min', '15min', '30min', '60min', 'daily',
        'weekly', 'monthly'] (default 'daily')
    outputsize : str
        The size of the call, supported values are
        'compact' and 'full; the first returns the last 100 points in the
        data series, and 'full' returns the full-length intraday times
        series, commonly above 1MB (default 'compact')
    alpha_vantage_key : default
        api key for Alpha Vantage
    intrinio_key : default
        api key for intrinio

    -----------------------------
    Attributes 
    -----------------------------
    """


    def __init__(self,
                 symbol,
                 interval='daily',
                 outputsize='full',
                 alpha_vantage_key=config('ALPHA_VANTAGE'), 
                 intrinio_key=config('INTRINIO_KEY')):

        self.symbol=symbol
        self.interval = interval
        self.outputsize=outputsize
        self.alpha_vantage_key = alpha_vantage_key
        self.intrinio_key = intrinio_key


    def security(self,  
                 supp_symbol=None, 
                 primary_df=None,
                 step='init',):
    
        # TODO ASSERT Error if step /= 'new' or 'add'
        # TODO ASSERT Error if step == 'add' and no supplimental ticker is provided
        # TODO ASSERT Error if step == 'add' and no primary dataframe is provided

        ts = TimeSeries(key=self.alpha_vantage_key, 
                        output_format='pandas')

        if step == 'init':
            symbol = self.symbol
        else:
            symbol = supp_symbol

        data, meta_data = ts.get_daily(symbol=symbol, 
                                       outputsize=self.outputsize)

        print(meta_data)

        data = data.rename(columns={
                '1. open'  : symbol+' open', 
                '2. high'  : symbol+' high', 
                '3. low'   : symbol+' low', 
                '4. close' : symbol+' close', 
                '5. volume': symbol+' volume'
        }
    )
        if step == 'init':
            return data

        else:
            final_df = primary_df.merge(data, 
                                        how='inner', 
                                        on='date')
            return final_df



 
