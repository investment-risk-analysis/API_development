from decouple import config


class Wrangle:
    """
    Class based data wrangling function. Uses the APIs to 
    import the data, then wrangles it together.

    -----------------------------
    Parameters
    -----------------------------
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
                 interval='daily',
                 outputsize='full',
                 alpha_vantage_key=config('ALPHA_VANTAGE'), 
                 intrinio_key=config('INTRINIO_KEY')):
        self.interval = interval
        self.alpha_vantage_key = alpha_vantage_key
        self.intinio_key = intrinio_key
        self.week = week