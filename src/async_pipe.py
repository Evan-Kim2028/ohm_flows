import asyncio
from queryportal.subgraphinterface import SubgraphInterface
from datetime import datetime, timedelta
import os
import polars as pl
pl.Config.set_fmt_str_lengths(200)

# make a folder called data
if not os.path.exists('data'):
    os.makedirs('data')

# Load decentralized endpoints
sgi = SubgraphInterface(endpoints={
    'univ3': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7',
    'balv2': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/Ei5typKWPepPSgqkaKf3p5bPhgJesnu1RuRpyt69Pcrx',
    'curve': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/GAGwGKc4ArNKKq9eFTcwgd1UGymvqhTier9Npqo1YvZB',
    'sushi': 'https://api.playgrounds.network/v1/proxy/subgraphs/id/7h1x51fyT5KigAhXd8sdE3kzzxQDJxxz1y66LTFiC3mS',
})

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
    'pool_id'
]

end_date = datetime(2021, 5, 22)
start_date = end_date - timedelta(days=21)

filter = {
    'timestamp_gte': int(start_date.timestamp()),
    'timestamp_lte': int(end_date.timestamp()),
}

query_size = 1500000
########################################################


########################################################
# ASYNC STUFF
def process_subgraph(subgraph):
    sgi.query_entity(
        query_size=query_size,
        entity='swaps',
        name=subgraph,
        query_paths=query_paths,
        filter_dict=filter,
        orderBy='timestamp',
        graphql_query_fmt=True,
        saved_file_name=f'data/{subgraph}_swaps_{start_date.strftime("%m-%d")}_{end_date.strftime("%m-%d")}'
        )



async def main():
    await asyncio.gather(*[asyncio.to_thread(process_subgraph, subgraph) for subgraph in list(sgi.subject.subgraphs.keys())])


# Run the asyncio event loop
asyncio.run(main())
########################################################