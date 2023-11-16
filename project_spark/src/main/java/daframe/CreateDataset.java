package daframe;
/**
 * PROGRAMMA PER LA CREAZIONE DI DATASET E DATAFRAME
 * @author Mary Bonomo
 */

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.*;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import daframe.DataFrameExample.Schema;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;

public class CreateDataset {
	public static void main(String[] args) throws IOException {
			
	SparkSession spark = SparkSession.builder()
			.appName("Dataset")
			.master("local[2]")
			.getOrCreate();
	
	spark.sparkContext().setLogLevel("ERROR");
	
	/**
	 * mode 1: caricamento elementi da una lista
	 */
	/*
	List<Row> list=new ArrayList<Row>();
	list.add(RowFactory.create("biologia"));
	list.add(RowFactory.create("matematica"));
	list.add(RowFactory.create("italiano"));
	list.add(RowFactory.create("inglese"));
	List<org.apache.spark.sql.types.StructField> listOfStructField=
			new ArrayList<org.apache.spark.sql.types.StructField>();
	listOfStructField.add(DataTypes.createStructField("Materie", DataTypes.StringType, true));
	
	StructType structType=DataTypes.createStructType(listOfStructField);
	Dataset<Row> data=spark.createDataFrame(list,structType);
	data.createOrReplaceTempView("materie");
	data.show();
	*/
	
	
	
	/**
	 * caricamento tramite interfaccia
	 */
	/*
	Dataset<Row> film= spark.createDataFrame(Arrays.asList(new Movie("movie1",10.2,"Excelsior"),
			new Movie("movie2",9.3,"cinema4 Sale"),
			new Movie("movie3",5.4,"Arena2 Sale"),
			new Movie("movie4",4.2,"Cinema Paradiso")
			), Movie.class);
	film.createOrReplaceTempView("Film");
	Long numero=film.where("rating>5").count();
	film.where("rating>5").show();
	film.select("cinema").show();
	System.out.println(numero);
	film.show();
	*/
	
	
	/**
	 * caricamento da file	products.csv   
	 */
	/*
	StructType schema = new StructType()
		    .add("id", "string")
		    .add("name", "string")
			.add("code", "string")
			.add("category", "string")
			.add("subcategory", "string")
			.add("price", "string")
			.add("currency", "string");
	
	Dataset<Row> df = spark.read()
			.schema(schema)		
			.csv("C:\\Users\\Mary\\eclipse-workspace\\projectDataset\\src\\main\\resources\\products.csv");
			
	df.createOrReplaceTempView("products");
	df.filter("price>100").show();
	df.where("price==399").groupBy("price").count().show();
	df.show();	  
*/
	
	
	
	//  ALTRO ESERCIZIO
	//utilizzo con schema
	
	
	StructType schema = new StructType()
		    .add("userId", "string")
		    .add("movieId", "string")
			.add("rating", "string")
			.add("timestamp", "string");
	
	
	
	Dataset<Row> film = spark.read()
			.schema(schema)
			.csv("C:\\Users\\Mary\\eclipse-workspace\\project_spark\\src\\main\\resources\\ratings.csv");
	
	Dataset<Row> film_prova = spark.read()
			.schema(schema)
			.csv("C:\\Users\\Mary\\eclipse-workspace\\project_spark\\src\\main\\resources\\ratings_II.csv");
	
	
	film_prova.createOrReplaceTempView("film");
	film_prova.show();
	
	//film_prova.select("_c0").show();
	//System.out.println(film.distinct().count());
	//film_prova.select("userID", "rating").filter("rating>4.0").distinct().show();
	//film_prova.filter("rating>4.0").distinct().show();
	//film_prova.filter("userId== 100").show();
	
	
	film.join(film_prova).distinct().show();
	film.intersect(film_prova).show();
	film.select("userId").intersect(film_prova).select("userId").show();
	Long intersezione=film.intersect(film_prova).count();
	//System.out.println("comuni: "+intersezione);
	
	
//**********************************************************************	
////ESERCIZIO DA FARE DA SOLI	
		
	/**
	 * caricamento da dataset malattie
	 */

	StructType struttura = new StructType()
    .add("id", "string")
	.add("name", "string")
	.add("score", "double");

Dataset<Row> malattie = spark.read()
	.schema(struttura)	
	.csv("C:\\Users\\Mary\\eclipse-workspace\\projectDataset\\src\\main\\resources\\data_mal.csv");
	
malattie.createOrReplaceTempView("malattie");
malattie.show(); // totale	
//malattie.groupBy("_c1").count().show();


///operazioni 
malattie.groupBy("score").count().show();
malattie.filter("score==100").show();
malattie.select("id","name","score").where("score>50").orderBy("name").show();


	
	
	}	
}
