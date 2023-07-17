# Michael Godfrey 2022 (c)
# michael.freaking.godfrey@gmail.com
# --
# Apache 2.0
# 
# src/lc.py
import datetime, pandas as pd
DF = pd.DataFrame
CONCAT = pd.concat
from dl import download_cache, download_short_cache

_LOG_LEVEL_EROR = 10
_LOG_LEVEL_WARN = 20
_LOG_LEVEL_INFO = 30
_LOG_LEVEL_DEBG = 40
_LOG_LEVEL_TIME = 9001
LOG_LEVEL = _LOG_LEVEL_DEBG
DO_LOG_TIMER = False

# PRINT_LOCK = threading.Lock()
def LOG(message,  level=_LOG_LEVEL_EROR, LOGGER=None): 
    #FIXME: with PRINT_LOCK:
    import gc
    if (level <= LOG_LEVEL) | ((level == _LOG_LEVEL_TIME) & DO_LOG_TIMER):
        msg = f"gc:({str(gc.get_count()):>14}) {datetime.datetime.now().strftime('|%Y/%m/%d %H:%M,%S|')} {message}"
        print(msg)
        if LOGGER != None:
            LOGGER(msg)
    
ERROR = lambda message : LOG("[ERROR]: " + message,_LOG_LEVEL_EROR)
WARN  = lambda message : LOG("[WARN ]: " + message,_LOG_LEVEL_WARN)

def INFO(message, LOGGER=None):
    LOG("[INFO ]: " + message,_LOG_LEVEL_INFO, LOGGER)
def DEBUG(message, LOGGER=None):
    LOG("[DEBUG]: " + message,_LOG_LEVEL_DEBG, LOGGER)

TIME  = lambda message : LOG("[timer]: " + message,_LOG_LEVEL_TIME)

def load_df_cache(sym_list, cache_period, CACHE_FILE_PREFIX, FORCE_REFRESH_CACHE = False, LOGGER=None):

  df_cache_dict = {}
  # Found to be unsupprted: "GALA", "ZRX"
  for sym in sym_list:
    DEBUG(f"Starting load_df_cache: {sym}", LOGGER)
    CACHE_FILENAME = f"{CACHE_FILE_PREFIX}_{sym}_{cache_period}.pqt"
    try:
      if FORCE_REFRESH_CACHE == True:
        raise "lol, ohai"
      # Load
      df_cache = pd.read_parquet(CACHE_FILENAME).sort_index()
      INFO(f"Using Cached, {CACHE_FILENAME}): {min(df_cache.index)} to {max(df_cache.index)}", LOGGER)
    except:
      # Download & Save
      DEBUG(f"Caching: {CACHE_FILENAME}",LOGGER)
      df_cache = download_cache([sym], cache_period, LOGGER=LOGGER)
      df_cache.to_parquet(CACHE_FILENAME, compression='gzip')
      INFO(f"Cached!: {CACHE_FILENAME}): {min(df_cache.index)} to {max(df_cache.index)}", LOGGER)
    short_df = download_short_cache([sym], cache_period, startAt=df_cache.index.max() - pd.Timedelta('1 h'), endAt=datetime.datetime.now(), LOGGER=LOGGER)
    INFO(f"Cache Updated: {CACHE_FILENAME}): {min(short_df.index)} to {max(short_df.index)}", LOGGER)
    df_cache.to_parquet(CACHE_FILENAME+".bak", compression='gzip')
    appended_df_cache =  CONCAT([df_cache, short_df]).sort_index().drop_duplicates()

    # NOTE: Not caching the last 12 hours, as datasource sometimes flubs more recent values
    # TODO: Plausibility check for newly appended data
    appended_df_cache.loc[:appended_df_cache.index.max() - pd.Timedelta('3 h')].to_parquet(CACHE_FILENAME, compression='gzip')
    df_cache_dict[sym] = appended_df_cache

  import functools as ft
  df_cache = ft.reduce(lambda left, right: pd.merge(left, right, on='Time', how='outer'), df_cache_dict.values()).sort_index()
  
  # Take only symbols with history extending from before avg
  _cacheList = pd.Series({_s:_d.index.min() for _s,_d in df_cache_dict.items()}).sort_values(ascending=True)
  OV_startAt   = _cacheList.max()
  OV_sym_index = _cacheList.index
  OV_df_cache  = df_cache.loc[OV_startAt:].copy()
  for c in df_cache.columns:
      if not (c[0] in OV_sym_index.values) or not c[1]: 
          OV_df_cache = OV_df_cache.drop(c, axis=1)

  # smooooth dataframe
  assert(OV_df_cache.index.is_monotonic_increasing)

  OV_df_cache.columns = pd.Index([f"{c[1]}_{c[0]}" for c in OV_df_cache.columns.values])
  OV_df_cache = OV_df_cache.drop_duplicates()

  # Imputation of missing values after merging rows
  for v in [f"Volume_{sym}" for sym in OV_sym_index]:
      OV_df_cache[v].fillna(value=0, inplace=True)
  for v in [f"Amount_{sym}" for sym in OV_sym_index]:
      OV_df_cache[v].fillna(value=0, inplace=True)
  for sym in OV_sym_index:
    OV_df_cache.loc[:,f"Open_{sym}" ].loc[OV_df_cache.loc[:,f"Open_{sym}" ].isna()] = OV_df_cache.loc[:,f"Close_{sym}"].shift(1).loc[OV_df_cache.loc[:,f"Open_{sym}"].isna()]
    OV_df_cache.loc[:,f"Open_{sym}" ].ffill(inplace=True)
    OV_df_cache.loc[:,f"Close_{sym}"].loc[OV_df_cache.loc[:,f"Close_{sym}"].isna()] = OV_df_cache.shift(1).loc[:,f"Close_{sym}"].loc[OV_df_cache.loc[:,f"Close_{sym}"].isna()]
    OV_df_cache.loc[:,f"High_{sym}" ].loc[OV_df_cache.loc[:,f"High_{sym}" ].isna()] = OV_df_cache.shift(1).loc[:,f"High_{sym}" ].loc[OV_df_cache.loc[:,f"High_{sym}" ].isna()]
    OV_df_cache.loc[:,f"Low_{sym}"  ].loc[OV_df_cache.loc[:,f"Low_{sym}"  ].isna()] = OV_df_cache.shift(1).loc[:,f"Low_{sym}"  ].loc[OV_df_cache.loc[:,f"Low_{sym}"  ].isna()]
  return OV_startAt, OV_sym_index, OV_df_cache, df_cache

def appended_cache(SYM_LIST, cache_period, CACHE_FILE_PREFIX,LOGGER):
    _, OV_sym_index, OV_df_cache, df_cache = load_df_cache(SYM_LIST, cache_period, CACHE_FILE_PREFIX, FORCE_REFRESH_CACHE=False, LOGGER=LOGGER)
    short_df = download_short_cache(SYM_LIST, cache_period, startAt=OV_df_cache.index.max() - pd.Timedelta('1 d'), endAt=datetime.datetime.now(), LOGGER=LOGGER)
    OV_df_cache =  CONCAT([OV_df_cache, short_df[OV_df_cache.columns].loc[[idx for idx in short_df.index if idx not in OV_df_cache.index]] ]).sort_index()
    return OV_df_cache, OV_sym_index

def Start(SYM_LIST, cache_period, CACHE_FILE_PREFIX,FORCE_REFRESH_CACHE=False, LOGGER=None):
    _, OV_sym_index, OV_df_cache, df_cache = load_df_cache(SYM_LIST, cache_period, CACHE_FILE_PREFIX, FORCE_REFRESH_CACHE=False, LOGGER=LOGGER)
    tuples = [(c.split('_')[1], c.split('_')[0]) for c in OV_df_cache.columns]
    df_cache = OV_df_cache.T.set_index(pd.MultiIndex.from_tuples(tuples, names=["sym", "value"])).T    
    return df_cache

if __name__ == "__main__":
    lc_dir = "lc/"
    lc_cache = Start(
       ["XRP", "ALICE", "SHIB", "LTC", "SOL", "ZEC"], 
       "15min", 
       f"{lc_dir}forest", 
       FORCE_REFRESH_CACHE=True)

    from pandasgui import show
    show(lc_cache)
    lc_cache