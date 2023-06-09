import asyncio
import itertools
from queryportal.subgraphinterface import SubgraphInterface
from datetime import datetime, timedelta
import os
import polars as pl
pl.Config.set_fmt_str_lengths(200)

# `async_pipe_dates.py` is used to query daily swap data from the largest DEXes on Ethereum via The Graphs decentralized network of indexers. 
# Note that the Univ2 dex data was excluded because the subgraph was not completely indexed at this time. 


# Load decentralized endpoints
sgi = SubgraphInterface(endpoints={
    'balv2': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/Ei5typKWPepPSgqkaKf3p5bPhgJesnu1RuRpyt69Pcrx',
    'curve': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/GAGwGKc4ArNKKq9eFTcwgd1UGymvqhTier9Npqo1YvZB',
    'sushi': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/7h1x51fyT5KigAhXd8sdE3kzzxQDJxxz1y66LTFiC3mS',
    'univ3': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7'
})

# make a folder called data
if not os.path.exists('data'):
    os.makedirs('data')

for subgraph in list(sgi.subject.subgraphs.keys()):
    os.makedirs(f'data/{subgraph}', exist_ok=True)


########################################################
# Query params

# Fields to be returned from the query
query_paths = [
    'hash',
    'to',
    'from',
    'blockNumber',
    'timestamp',
    'tokenIn_symbol',
    'tokenOut_symbol',
    'amountIn',
    'amountOut',
    'amountInUSD',
    'amountOutUSD',
    'pool_id'
]



query_size = 200000

# ASYNC STUFF
def process_subgraph(subgraph, start_date, end_date):

    filter = {
        'timestamp_gte': int(start_date.timestamp()),
        'timestamp_lte': int(end_date.timestamp()),
    }

    sgi.query_entity(
        query_size=query_size,
        entity='swaps',
        name=subgraph,
        query_paths=query_paths,
        filter_dict=filter,
        orderBy='timestamp',
        # graphql_query_fmt=True,
        saved_file_name=f'data/{subgraph}/{subgraph}_swaps_{start_date.strftime("%m-%d")}_{end_date.strftime("%m-%d")}'
        )
    
    print(f'queried {subgraph} from {start_date.strftime("%m-%d")} to {end_date.strftime("%m-%d")}')




async def main():
    subgraph_keys = list(sgi.subject.subgraphs.keys())
    date_ranges = [(start_date, start_date + timedelta(days=1)) for start_date in [datetime(2023, 3, 1) + timedelta(days=i) for i in range(0, 90, 1)]]

    await asyncio.gather(*[asyncio.to_thread(process_subgraph, subgraph, start_date, end_date) for subgraph, (start_date, end_date) in itertools.product(subgraph_keys, date_ranges)])


# Run the asyncio event loop
asyncio.run(main())
########################################################

# starting at 10861 queries