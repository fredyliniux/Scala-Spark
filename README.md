# Exercices Spark

## Example 1

Given the files contained in the comics folder
1. To the Dataframe that contains the names of comics we want to add a column that contains the characters as an array
    
## Example 2
Given the file players_20.csvn our coach needs to know
1. What are the 20 players under 23 who have the most potential?
2. Which are the top 20 teams with the highest overall average?
3. Of the 20 previous teams our coach also wants to know which are the 3 goalkeepers and the 3 forwards with the best (overall)
4. Rank the players by nationality in such a way that we identify the top 5 from each country.

## Example 3
Given the PokemonData.csv file, read it as DataFrame (RDDs are recommended for initial reading)
1. Filtering by the types of pokemon (type1) fire and water it is necessary to calculate the average of each of the following columns:
    sp_attack, sp_defense and speed; in such a way that the resulting dataframe shows the following data:
    generation, avg_sp_attack_water, avg_sp_attack_fire, avg_sp_defense_water, avg_sp_defense_fire, avg_speed_water, avg_speed_fire

### Results

Each of the resulting Dataframes must be written in parquet format in a folder inside src / main / output / parquet /
# spark-scala
# spark-scala
