from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, broadcast
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

# Disable auto broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Load the data
match_details = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")
matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
medals_matches_players = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")
medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")

# Explicitly set the broadcast join threshold
broadcasted_medals = broadcast(medals)
broadcasted_maps = broadcast(maps)

# Join the data with broadcast join
broadcasted_medal_maps = medals_matches_players.join(broadcasted_medals, medals_matches_players["medal_id"] == broadcasted_medals["medal_id"], "inner") \
                         .join(broadcasted_maps, medals_matches_players["map_id"] == broadcasted_maps["map_id"], "inner")

# Bucket the data with 16 buckets
match_details_bucketed = match_details.write.bucketBy(16, "match_id").\
    sortBy("match_id").\
    join(matches, "match_id").\
    join(medals_matches_players, "match_id").\
    saveAsTable("bootcamp.match_details_bucketed")

# Aggregation
avg_kills_df = match_details_bucketed.groupBy("player_id").agg(
    expr("avg(kills) as avg_kills_per_game"))
most_playlist_df = match_details_bucketed.groupBy("playlist_id").count().orderBy(col("count").desc()).limit(1)
most_map_df = match_details_bucketed.groupBy("map_name").count().orderBy(col("count").desc()).limit(1)
most_killing_spree_map_df = match_details_bucketed.filter(col("classification") == "KillingSpree").groupBy("map_name").agg(
    expr("count(classification) as max_killing_spree")).orderBy(col("max_killing_spree").desc()).limit(1)

# Show the result
avg_kills_df.show()
most_playlist_df.show()
most_map_df.show()
most_killing_spree_map_df.show()

# Repartition and sort
sorted = most_playlist_df.repartition(10, col("playlist_id"))\
    .sortWithinPartitions(col("playlist_id"))

sortedTwo = most_map_df.repartition(10, col("map_id"))\
    .sort(col("map_id"))

sortedThree = most_killing_spree_map_df.repartition(10, col("map_id"))\
    .sort(col("map_id"))

sorted.show()
sortedTwo.show()
sortedThree.show()