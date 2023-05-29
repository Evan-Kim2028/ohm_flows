from queryportal.subgraphinterface import SubgraphInterface
from datetime import datetime, timedelta
import os

import polars as pl
pl.Config.set_fmt_str_lengths(200)


# make a folder called data
if not os.path.exists('data'):
    os.makedirs('data')


# Decentralized
sgi = SubgraphInterface(endpoints='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2')


query_paths = [
    'transaction_id',
    'pair_token0_symbol',
    'pair_token1_symbol',
    'amount0In',
    'amount0Out',
    'amount1In',
    'amount1Out',
    'amountUSD',
    # 'from',
    'id',
    'to',
    'timestamp'
]


end_date = datetime(2021, 5, 29)
start_date = end_date - timedelta(days=7)

filter = {
    'timestamp_gte': int(start_date.timestamp()),
    'timestamp_lte': int(end_date.timestamp()),
}

query_size = 1000000

univ2 = sgi.query_entity(
    query_size=query_size,
    entity='swaps',
    name='uniswap-v2',
    query_paths=query_paths,
    filter_dict=filter,
    orderBy='timestamp',
    saved_file_name=f'data/univ2_swaps_{start_date.strftime("%m-%d")}_{end_date.strftime("%m-%d")}'
    )