import nflreadpy as nfl
import polars as pl

schedule_stats = nfl.load_schedules()

filtered_columns = schedule_stats.filter(
            (pl.col("result").is_not_null()),
            (pl.col("away_team").str.contains("DEN|CAR|DAL")) | (pl.col("home_team").str.contains("DEN|CAR|DAL"))
        ).select(
            pl.col("season"),pl.col("week"),pl.col("result"),pl.col("away_team"), pl.col("home_team"))

print(filtered_columns)