val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType;

val df  =  spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/pokemon/PokemonData.csv")

df.registerTempTable("pokemon")


//importamos SparkConf, SparkContext, RDD
import org.apache.spark.SparkConf 
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

sc
res102: org.apache.spark.SparkContext = org.apache.spark.SparkContext@76b6e011

//se obtiene la configuración actual de SparkConf
sc.getConf.getAll


pokemonRDD = sc.textFile("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/pokemon/PokemonData.csv").map(lambda line  : line.split(","))

pokemonRDD = spark.read.option("header","true").csv("/Users/fredyreyes/Documents/examen_diagnostico_datio/src/main/resources/input/csv/pokemon/PokemonData.csv")