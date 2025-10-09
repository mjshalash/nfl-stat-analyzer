import nflreadpy as nfl
import polars as pl

# Set the maximum number of rows to display to 50
pl.Config.set_tbl_rows(1000)

schedule_stats = nfl.load_schedules()

filtered_columns = schedule_stats.filter(
            (pl.col("result").is_not_null()),
            (pl.col("away_team").str.contains("DEN|CAR|DAL")) | (pl.col("home_team").str.contains("DEN|CAR|DAL"))
        ).select(
            ["season","week","result","away_team","home_team"]
        ).with_columns(
            pl.when(pl.col("result") > 0).then("home_team").otherwise("away_team").alias("winner")
        ).filter(pl.col("winner").str.contains("DEN|CAR|DAL"))

print(filtered_columns)