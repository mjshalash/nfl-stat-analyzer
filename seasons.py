import nflreadpy as nfl
import polars as pl

schedule_stats = nfl.load_schedules()

filtered_columns = schedule_stats.filter(
            (pl.col("result").is_not_null()),
            (pl.col("away_team").str.contains("DEN|CAR|DAL")) | (pl.col("home_team").str.contains("DEN|CAR|DAL"))
        ).select(
            ["season","week","result","away_team","home_team"]
        ).with_columns(pl.when(pl.col("result") > 0).then("home_team").otherwise("away_team").alias("Winner"))

print(filtered_columns)