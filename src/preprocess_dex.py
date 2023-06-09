import polars as pl
import os

pl.Config.set_fmt_str_lengths(200)

# `preprocess_dex.py` is used to concatenate all the daily parquet files into one big parquet file and then concat all the big parquet files into an aggregate dex swaps parquet file.


dfs_agg = []

dfs_dex_agg = []

for subfolder in ['balv2', 'curve', 'sushi', 'univ3']:
    for file in os.listdir(f'data/{subfolder}'):
        # print(f'{file}: {os.path.getsize(f"data/{subfolder}/{file}") / 1e6} MB')
        daily_df = pl.read_parquet(f'data/{subfolder}/{file}')
        # print(f'df size: {df.shape}')
        dfs_agg.append(daily_df)


    # concat all daily_dfs
    dexes = pl.concat(dfs_agg)

    #add subfolder column name
    dexes = dexes.with_columns(pl.lit(subfolder).alias('dex'))

    # sort by timestamp
    dexes = dexes.sort('timestamp')

    dfs_dex_agg.append(dexes)


dexes_agg = pl.concat(dfs_dex_agg)

# convert timestamp to datetime
dexes_agg = dexes_agg.with_columns(
       pl.from_epoch("timestamp")
   )

# save dexes to dex_swaps folder
dexes_agg.write_parquet(f'data/dexes_agg.parquet')

print(f'final df size: {dexes_agg.shape}')
print(dexes_agg.tail(5))