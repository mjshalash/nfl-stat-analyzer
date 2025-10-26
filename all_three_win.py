import nflreadpy as nfl
import polars as pl

# Set the maximum number of rows to display to 50
pl.Config.set_tbl_rows(1000)

schedule_stats = nfl.load_schedules()
#TODO: Need to filter out any times that DEN, CAR or DAL were playing each other, as this would not provide an accurate week to count as it would have been impossible for all 3 to win
#TODO: Create file for base dataframe that we can then import into other files as needed, to avoid copying
for s in range(1999,2026):
    for w in range (1,19):
        filtered_columns = schedule_stats.filter(
                    (
                        pl.col("result").is_not_null()
                    ),
                    (~((pl.col("away_team").str.contains("DEN|CAR|DAL")) & 
                        (pl.col("home_team").str.contains("DEN|CAR|DAL")))
                    ),
                ).with_columns(
                    pl.when(pl.col("result") > 0).then("home_team").otherwise("away_team").alias("winner")
                ).select(
                    ["season","week","winner"]
                ).filter(
                    pl.col("winner").str.contains("DEN|CAR|DAL")
                ).filter(
                    pl.col("season").eq(s)
                ).filter(
                    pl.col("week").eq(w)
                )
        print(filtered_columns)

        # if (filtered_columns.height == 0):
        #     print(s, "season", "|| Week ", w)