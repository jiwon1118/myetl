import pandas as pd

def load_parquet(dis):
    dir_path = "/home/jiwon/data/"
    df = pd.read_csv(f"{dir_path}{dis}/data.csv")
    df.to_parquet(f"{dir_path}{dis}/data.parquet", engine='pyarrow')