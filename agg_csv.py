import pandas as pd

def agg_csv(dis):
    dir_path = "/home/jiwon/data/"
    df = pd.read_parquet(f"{dir_path}{dis}/data.parquet", engine='pyarrow')
    gdf = df.groupby(["name", "value"]).size().reset_index(name="count")
    gdf.to_csv(f"{dir_path}{dis}/agg.csv", index=False)
    