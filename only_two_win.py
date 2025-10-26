import nflreadpy as nfl
import polars as pl

# Set the maximum number of rows to display to 50
pl.Config.set_tbl_rows(1000)

schedule_stats = nfl.load_schedules()

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
                            ["season","week","away_team","home_team","winner"]
                        ).filter(
                            pl.col("winner").str.contains("DEN|CAR|DAL")
                        ).filter(
                            pl.col("season").eq(s)
                        ).filter(
                            pl.col("week").eq(w)
                        )

        df_dict = filtered_columns.to_dict()

        # TODO: Need a way to list out the two winners and the loser result so we can analyze who has prevented the trifecta the most, etc. etc.
        if (filtered_columns.height == 2):
            print(s, "season", "|| Week ", w)
            print(filtered_columns)