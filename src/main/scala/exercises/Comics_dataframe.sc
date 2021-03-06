
// For this exersice is necessary start spark-shell and execute this script

//import 

import sqlContext.implicits._
import org.apache.spark.sql.functions.col

import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType;

//crete sqlContext vatiable

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

import sqlContext.implicits._


// Read first CSV and save in variable called dataset
val dataset =  spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/comics/comics.csv")

// Query to consult : Temporal table 1.
/*spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/comics/comics.csv").show(5)
+-------+--------------------+-----------+--------------------+
|comicID|               title|issueNumber|         description|
+-------+--------------------+-----------+--------------------+
|  16232|Cap Transport (20...|         12|                null|
|  16248|Cap Transport (20...|          9|                null|
|   4990| Halo Preview (2006)|          0|                null|
|  21486|Ultimate X-Men (S...|          9|                null|
|  58634|A Year of Marvels...|          5|It’s Halloween in...|
+-------+--------------------+-----------+--------------------+
only showing top 5 rows
*/

// Read second CSV and save in variable called dataset3
val dataset3 =  spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/comics/charactersToComics.csv")

// Query to consult : Temporal table 2.
/*spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/comics/charactersToComics.csv").show(5)
+-------+-----------+
|comicID|characterID|
+-------+-----------+
|  16232|    1009220|
|  16232|    1010740|
|  16248|    1009220|
|  16248|    1009471|
|  16248|    1009552|
+-------+-----------+
only showing top 5 rows
*/

// Read third CSV and save in variable called dataset2
val dataset2 =  spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/comics/characters.csv")

// Query to consult : Temporal table 3.
/*spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/comics/characters.csv").show(5)
+-----------+---------------+
|characterID|           name|
+-----------+---------------+
|    1009220|Captain America|
|    1010740| Winter Soldier|
|    1009471|      Nick Fury|
|    1009552|   S.H.I.E.L.D.|
|    1009228|  Sharon Carter|
+-----------+---------------+
only showing top 5 rows
*/

// Create join with dataset, dataset3 and dataset 2 and save in fft2 variable
val fft2 = dataset.join(dataset3,"comicID").join(dataset2,"characterID")

// Query to consult : Join
/*dataset.join(dataset3,"comicID").join(dataset2,"characterID").show(5)
+-----------+-------+--------------------+-----------+-----------+---------------+
|characterID|comicID|               title|issueNumber|description|           name|
+-----------+-------+--------------------+-----------+-----------+---------------+
|    1010740|  16232|Cap Transport (20...|         12|       null| Winter Soldier|
|    1009220|  16232|Cap Transport (20...|         12|       null|Captain America|
|    1009228|  16248|Cap Transport (20...|          9|       null|  Sharon Carter|
|    1009552|  16248|Cap Transport (20...|          9|       null|   S.H.I.E.L.D.|
|    1009471|  16248|Cap Transport (20...|          9|       null|      Nick Fury|
+-----------+-------+--------------------+-----------+-----------+---------------+
only showing top 5 rows
*/

// Select new dataset  and save in sqlResult variable
val sqlResult = fft2.select("comicID","title","issueNumber","description","name")

//Query to show dataframe 
/*fft2.select("comicID","title","issueNumber","description","name").show(10)
+-------+--------------------+-----------+--------------------+--------------------+
|comicID|               title|issueNumber|         description|                name|
+-------+--------------------+-----------+--------------------+--------------------+
|  16232|Cap Transport (20...|         12|                null|      Winter Soldier|
|  16232|Cap Transport (20...|         12|                null|     Captain America|
|  16248|Cap Transport (20...|          9|                null|       Sharon Carter|
|  16248|Cap Transport (20...|          9|                null|        S.H.I.E.L.D.|
|  16248|Cap Transport (20...|          9|                null|           Nick Fury|
|  16248|Cap Transport (20...|          9|                null|     Captain America|
|  21486|Ultimate X-Men (S...|          9|                null|    X-Men (Ultimate)|
|  58634|A Year of Marvels...|          5|It’s Halloween in...|            Punisher|
|  58634|A Year of Marvels...|          5|It’s Halloween in...|Hawkeye (Kate Bis...|
|  16241|Cap Transport (20...|         20|                null|            Spitfire|
+-------+--------------------+-----------+--------------------+--------------------+
only showing top 10 rows
*/

//Write query result and save as Task1_ex1 in parquet format
sqlResult.write.format("parquet").save("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/Output/Parquet/Task1_ex1")

