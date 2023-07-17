# (c) 2023 - michael.freaking.godfrey@gmail.com
#
# Apache 2.0
# 
# src/dl.py
import requests, time
from requests import get; from requests.adapters import HTTPAdapter, Retry
from datetime import datetime
import pandas as pd
DF = pd.DataFrame
CONCAT = pd.concat

url = 'https://api.kucoin.com'
def getKuCoinOHLCV(period:str,symbol:str,startAt:datetime,endAt:datetime,pages_per_heartbeat:int=10,heartbeat:float=1.23, LOGGER=None) -> pd.DataFrame:
  """
  Description:
      Get OHLCV candlesticks from the KuCoin API

  Parameters:
      period (String): OHLC candlestick period
      symbol (String): Symbol for security (compared w/ USDT)
      startAt (datetime): Start date
      endAt(datetime): End Date

  Gotchas:
      USDT is hardcoded as pair base

  Returns:
      Pandas Dataframe 

  Usage Example:
    `getKuCoinOHLCV("1hour", "BTC", datetime(2021,2,1), datetime(2021,7,1))`
  """
  nrecs = 0
  df = DF()
  s = requests.Session()
  
  retries = Retry(total=500, backoff_factor=1, status_forcelist=[ 500, 502, 503, 504, 429, 429000, 200000 ])
  # KuCoin API limits results to 15,000 records so paginate accordingly as needed
  page_count = 0
  while endAt > startAt:
    try:
      s.mount('https://', HTTPAdapter(max_retries=retries))
      response = s.get(url + '/api/v1/market/candles?type={}&symbol={}-USDT&startAt={:.0f}&endAt={:.0f}'.format(period, symbol, startAt.timestamp(), endAt.timestamp()))
      df_part = DF(response.json()['data'])
      if not response.json()['data']:
        break
      df_part = df_part.rename({
        0:"Time", 
        1:"Open", 
        2:"Close", 
        3:"High", 
        4:"Low", 
        5:"Amount", 
        6:"Volume"
      }, axis='columns')

      df_part['Time']   = df_part['Time'  ].apply(int).apply(datetime.fromtimestamp)
      df_part['Open']   = df_part['Open'  ].apply(float)
      df_part['Close']  = df_part['Close' ].apply(float)
      df_part['High']   = df_part['High'  ].apply(float)
      df_part['Low']    = df_part['Low'   ].apply(float)
      df_part['Amount'] = df_part['Amount'].apply(float)
      df_part['Volume'] = df_part['Volume'].apply(float)
      df_part.set_index('Time', inplace=True)

      df_part = df_part.groupby(['Time']).agg(
         Open  = ('Open'  , 'last' ),
         Close = ('Close' , 'first'),
         High  = ('High'  , 'max'  ),
         Low   = ('Low'   , 'min'  ),
        Amount = ('Amount', 'sum'  ),
        Volume = ('Volume', 'sum'  )
      )

      df = CONCAT([df,df_part])
      nrecs += len(df_part)
      endAt = min(df_part.index)
      # add 1 hour to endAt
      endAt = endAt
      page_count += 1
      time.sleep(0.5)
    except:
      print(response.json())
      raise

  return df.sort_index()

def download_short_cache(syms, freq, startAt, endAt, LOGGER=None):
  # Downloading:
  df_cache = CONCAT({
      sym: getKuCoinOHLCV( freq,  sym,  startAt, endAt, LOGGER=LOGGER) for sym in syms 
    }).reset_index(level=0).rename(columns={'level_0':'sym'}).pivot(columns=['sym']).T.swaplevel().T

  # Light Sanitizing:
  sym = syms[0]
  df_cache.loc[:,(sym, "Open") ].loc[df_cache[sym]["Open"].isna()] = (lambda _df: _df.shift(1)[f'Close'].loc[df_cache[sym]["Open"].isna()])(df_cache[sym])
  df_cache.loc[:,(sym, "Open") ].ffill(inplace=True)
  df_cache.loc[:,(sym, "Close")].loc[df_cache.loc[:,(sym,"Close")].isna()] = (lambda _df: _df.shift(1)[f"Close"].loc[df_cache.loc[:,(sym,"Close")].isna()])(df_cache[sym])
  df_cache.loc[:,(sym, "High") ].loc[df_cache.loc[:,(sym,"High") ].isna()] = (lambda _df: _df.shift(1)[f"High" ].loc[df_cache.loc[:,(sym,"High") ].isna()])(df_cache[sym])
  df_cache.loc[:,(sym, "Low")  ].loc[df_cache.loc[:,(sym,"Low")  ].isna()] = (lambda _df: _df.shift(1)[f"Low"  ].loc[df_cache.loc[:,(sym,"Low")  ].isna()])(df_cache[sym])

  for col_name in ["Open", "Close", "High", "Low"]:
    df_cache.loc[:,(sym, col_name)] = df_cache.loc[:,(sym,col_name)]
  for col_name in ["Amount", "Volume"]:
    df_cache.loc[:,(sym,col_name)] = df_cache.loc[:,(sym,col_name)].fillna(0)
  return df_cache

def download_cache(syms, freq, LOGGER=None):
  ''' download_cache()

  Downloads complete history from around October 2017 to current date
  (takes a while, like 10 minutes)
  (TODO: save cache as Base64 string)
  '''

  # Downloading:
  year_dl_pages = [datetime(year+1,1,1) for year in range(2016, datetime.now().year)] + [datetime.now()]
  df_cache = CONCAT({ 
    sym : CONCAT([cache_dict[sym] for cache_dict in [{
      sym: getKuCoinOHLCV( freq,  sym,  year_dl_pages[idx], year_dl_pages[idx+1], LOGGER=None) for sym in syms 
    } for idx in range(len(year_dl_pages)-1)]], axis=0) for sym in syms}, axis=1).sort_index()

  # Light Sanitizing:
  sym = syms[0]
  df_cache.loc[:,(sym, "Open") ].loc[df_cache[sym]["Open"].isna()] = (lambda _df: _df.shift(1)[f'Close'].loc[df_cache[sym]["Open"].isna()])(df_cache[sym])
  df_cache.loc[:,(sym, "Open") ].ffill(inplace=True)
  df_cache.loc[:,(sym, "Close")].loc[df_cache.loc[:,(sym,"Close")].isna()] = (lambda _df: _df.shift(1)[f"Close"].loc[df_cache.loc[:,(sym,"Close")].isna()])(df_cache[sym])
  df_cache.loc[:,(sym, "High") ].loc[df_cache.loc[:,(sym,"High") ].isna()] = (lambda _df: _df.shift(1)[f"High" ].loc[df_cache.loc[:,(sym,"High") ].isna()])(df_cache[sym])
  df_cache.loc[:,(sym, "Low")  ].loc[df_cache.loc[:,(sym,"Low")  ].isna()] = (lambda _df: _df.shift(1)[f"Low"  ].loc[df_cache.loc[:,(sym,"Low")  ].isna()])(df_cache[sym])

  for col_name in ["Open", "Close", "High", "Low"]:
    df_cache.loc[:,(sym, col_name)] = df_cache.loc[:,(sym,col_name)]
  for col_name in ["Amount", "Volume"]:
    df_cache.loc[:,(sym,col_name)] = df_cache.loc[:,(sym,col_name)].fillna(0)

  return df_cache