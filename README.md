lc - Caching Data Downloader for Kucoin API
--

First call to `Start` will take a long time, subsequent calls will be really fast

## How To:

### Download Data

#### Usage
```python
    import lc

    period, lc_dir = "15min", "lc/"
    symbols = ["SOL", "CAKE", "ETH", "BTC"]
    FILE_PREFIX = f"{lc_dir}forest"
    OV_df_cache, OV_sym_index = lc.Start(symbols, period, FILE_PREFIX, FORCE_REFRESH_CACHE=True)

    OV_df_cache

```

#### Example Output:

```
[DEBUG]: Starting load_df_cache: SHIB
[INFO ]: Using Cached, lc/forest_SHIB_15min.pqt): 2021-05-10 03:00:00 to 2023-07-16 19:45:00
[INFO ]: Cache Updated: lc/forest_SHIB_15min.pqt): 2023-07-16 14:45:00 to 2023-07-16 22:45:00
[DEBUG]: Starting load_df_cache: LTC
[INFO ]: Using Cached, lc/forest_LTC_15min.pqt): 2018-01-28 05:45:00 to 2023-07-16 19:45:00
[INFO ]: Cache Updated: lc/forest_LTC_15min.pqt): 2023-07-16 14:45:00 to 2023-07-16 22:45:00
[DEBUG]: Starting load_df_cache: SOL
[INFO ]: Using Cached, lc/forest_SOL_15min.pqt): 2021-08-04 06:00:00 to 2023-07-16 19:45:00
[INFO ]: Cache Updated: lc/forest_SOL_15min.pqt): 2023-07-16 14:45:00 to 2023-07-16 22:45:00
[DEBUG]: Starting load_df_cache: ZEC
[INFO ]: Using Cached, lc/forest_ZEC_15min.pqt): 2019-07-03 07:30:00 to 2023-07-16 19:45:00
[INFO ]: Cache Updated: lc/forest_ZEC_15min.pqt): 2023-07-16 14:45:00 to 2023-07-16 22:45:00

```

### Visualize with Pandas GUI
``` python
    from pandasgui import show
    show(OV_df_cache)
```