import gc
import pandas as pd

__all__ = ["delete_df"]

def delete_df(data:pd.DataFrame):
    del data
    gc.collect()