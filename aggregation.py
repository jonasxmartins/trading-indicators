import pandas as pd
from datetime import datetime

def read_sas_in_chunks(filename, chunksize):
    reader = pd.read_sas(filename, iterator=True, chunksize=chunksize)
    dfs = []  
    # i = 0
    for chunk in reader:
        dfs.append(chunk)
        # i += 1
        # if(i > 20):
        #     break

    df = pd.concat(dfs, ignore_index=True)
    return df

def compute_reset_cumsum(df):
    
    # computes reset cumsum and bars for one group
    def compute_for_group(group):
        cumsum = group['DollarVolume'].cumsum()
        bars = (cumsum / 1000000).astype(int)
        reset_cumsum = cumsum - bars * 1000000
        
        group['CumDollarVolume'] = reset_cumsum
        group['BarSample'] = bars
        return group
    
    # apply compute_for_group to groups
    result_df = df.groupby('SYM_ROOT').apply(compute_for_group).reset_index(drop=True)
    
    return result_df

# data Loading
data = read_sas_in_chunks('input.sas7bdat', chunksize=10000) # adjust chunksize based on memory
data['SYM_ROOT'] = data['SYM_ROOT'].str.decode('utf-8')

# filters out the necessary identifiers
#brk = 'BRK' + '.' + 'A'
data = data[data.SYM_ROOT.isin(['AAPL', 'TSLA', 'BRK'])]

#this processes the data according to dollar bar sampling 
def process_chunk(df):
    #calculate Dollar Volume
    df['DollarVolume'] = df['PRICE'] * df['SIZE']
    df = compute_reset_cumsum(df)
    
    # aggregation
    agg_funcs = {
        'TIME_M': ['first', 'last', 'count'],
        'PRICE': ['first', 'max', 'min', 'last'],
        'SIZE': 'sum'
    }
    agg_df = df.groupby(['SYM_ROOT', 'DATE', 'BarSample']).agg(agg_funcs).reset_index()

    df['SYM_ROOT'] = f"{df['SYM_ROOT']}.{df['SYM_SUFFIX']}"

    agg_df.columns = ['identifier', 'DATE', 'time_index', 'first_trade_time', 'last_trade_time', 'number_of_trades',
                      'open', 'high', 'low', 'close', 'volume']
    agg_df['DATE'] = pd.to_datetime(agg_df['DATE']).astype('datetime64[us]')
    agg_df['first_trade_time'] = pd.to_datetime(agg_df['first_trade_time']).astype('datetime64[us]')
    agg_df['last_trade_time'] = pd.to_datetime(agg_df['last_trade_time']).astype('datetime64[us]')


    return agg_df

result = process_chunk(data)

#saving
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
result.to_parquet(f'output_{timestamp}.parquet', index=False)

print(result.head(25))
