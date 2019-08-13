from alpha_vantage.foreignexchange import ForeignExchange
from alpha_vantage.techindicators import TechIndicators
from alpha_vantage.timeseries import TimeSeries
from decouple import config
import pandas as pd
import quandl

#pylint: disable=no-member
#pylint: disable=unbalanced-tuple-unpacking

#TODO ASSERT Testing
#TODO Print Statements
#TODO Clean Documentation
#TODO Find TWEXB
#TODO Other Macro from Hira

class Wrangle:
    """
    Class based data wrangling function. Uses the APIs to 
    import the data, then wrangles it together.
    
    -----------------------------
    Attributes
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
    Methods 
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


    def initiate(self, supp_symbol=None, primary_df=None, step='init',):
    
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

    def bulk_add_securities(self, symbols, primary_df):
        # TODO ASSERT symbols is a list:

        ts = TimeSeries(key=self.alpha_vantage_key, 
                        output_format='pandas')

        i_count = 0 

        for symbol in symbols:

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
            if i_count == 0:
                final_df = primary_df.merge(data, 
                                            how='inner', 
                                            on='date')
            else:
                final_df = final_df.merge(data,
                                          how='inner',
                                          on='date')
            
            i_count+=1

        return final_df

    def add_technicals(self, tech_symbols, primary_df, supp_symbol=None):

        if supp_symbol ==  None:
            symbol = self.symbol
        else:
            symbol = supp_symbol
            
        ti = TechIndicators(key=config('ALPHA_VANTAGE'), 
                            output_format='pandas')
        
        new_indicators = ["get_"+symbol.lower() for symbol in tech_symbols]

        i_count = 0 

        for ind in new_indicators:

            data, meta_data = getattr(ti, ind)(symbol=symbol)

            print(meta_data)

            data = data.rename(columns={
                    symbol : self.symbol+'_'+symbol, 
            }
        )
            if i_count == 0:
                final_df = primary_df.merge(data, 
                                            how='inner', 
                                            on='date')
            else:
                final_df = final_df.merge(data,
                                        how='inner',
                                        on='date')

            i_count+=1
      
        return final_df

    def add_forex(self, from_currency, to_currency, primary_df):
        
        cc = ForeignExchange(key=config('ALPHA_VANTAGE'), 
                             output_format='pandas')
        
        data, meta_data = cc.get_currency_exchange_daily(from_symbol=from_currency, 
                                                         to_symbol=to_currency)

        print(meta_data)

        data = data.rename(columns={
            '1. open'  : from_currency+'_to_'+to_currency+'_open', 
            '2. high'  : from_currency+'_to_'+to_currency+'_high', 
            '3. low'   : from_currency+'_to_'+to_currency+'_low', 
            '4. close' : from_currency+'_to_'+to_currency+'_close', 
        }
    )
        
        final_df = primary_df.merge(data,
                                    how='inner',
                                    on='date')

        return final_df

    def add_treasury_bonds(self, primary_df):

        data = quandl.get("USTREASURY/BILLRATES", 
                        authtoken=config('QUANDL_KEY'))

        print("US Treasury Bond Rates Added")

        final_df = primary_df.merge(data,
                                    how='inner',
                                    on='date')

        return final_df









 
