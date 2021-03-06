//import all dependencies

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lit

//Read  CSV finle
val df  =  spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/pokemon/PokemonData.csv")

//Dedine a Temporal table
df.registerTempTable("pokemon")

// Declare a variable prueba ann filter data

val prueba = sqlContext.sql("SELECT generation, type1, sp_attack, sp_defense, speed FROM pokemon WHERE type1 = 'fire'OR type1 = 'water' ")
/*
+----------+-----+---------+----------+-----+
|generation|type1|sp_attack|sp_defense|speed|
+----------+-----+---------+----------+-----+
|         1| fire|       60|        50|   65|
|         1| fire|       80|        65|   80|
|         1| fire|      159|       115|  100|
|         1|water|       50|        64|   43|
|         1|water|       65|        80|   58|
|         1|water|      135|       115|   78|
|         1| fire|       50|        65|   65|
|         1| fire|       81|       100|  109|
|         1|water|       65|        50|   55|
|         1|water|       95|        80|   85|
|         1| fire|       70|        50|   60|
|         1| fire|      100|        80|   95|
|         1|water|       40|        40|   90|
|         1|water|       50|        50|   90|
|         1|water|       70|        90|   70|
|         1|water|       50|       100|   70|
|         1|water|       80|       120|  100|
|         1| fire|       65|        65|   90|
|         1| fire|       80|        80|  105|
|         1|water|       40|        40|   15|
+----------+-----+---------+----------+-----+
only showing top 20 rows
*/

//val generation = Window.partitionBy('type1')
val type1 =Window.partitionBy("type1")

// First filter with Window to calculate average of sp_attack


// Add column  sp_attack as avg_sp_spttack  and  save in  a variable called avg_attack
val avg_attack = prueba.withColumn("avg_sp_attack", avg('sp_attack) over type1)

// Add column  sp_defense as avg_sp_defense  and  save in  a variable called avg_defense
val avg_defense = avg_attack.withColumn("avg_sp_defense", avg('sp_defense) over  type1)

// Add column  speed as avg_speed  and  save in  a variable called avg_speed
val avg_speed = avg_defense.withColumn("avg_speed", avg('speed) over type1)

//Select new dataset  and save in result variable
val result = avg_speed.select("generation","type1","avg_sp_attack","avg_sp_defense","avg_speed")

//Write query result and save as Task3_ex1 in parquet format

result.write.format("parquet").save("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/Output/Parquet/Task3_ex1")
