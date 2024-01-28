import gc
import pandas as pd

def delete_df(data:pd.DataFrame):
    del data
    gc.collect()