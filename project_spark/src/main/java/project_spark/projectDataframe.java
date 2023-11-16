package project_spark;

import java.io.IOException;
import java.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

//import com.esotericsoftware.kryo.util.Util;

import scala.Tuple2;

public class projectDataframe {
	SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
	JavaSparkContext sc =new JavaSparkContext(conf);
	//SparkSession sparkSession =SparkSession.builder().config(conf).getOrCreate();

	public projectDataframe() {
		
        this.sc.setLogLevel("ERROR");
    }
	
	public void filterValue() {		
		JavaSparkContext sc =new JavaSparkContext(conf);
		List<Double> inputData= new ArrayList<>();
		inputData.add(35.5);
		inputData.add(12.49943);
		inputData.add(90.32);
		inputData.add(20.32);
		
		JavaRDD<Double> myRDD =sc.parallelize(inputData);
		System.out.println(myRDD.collect());	
		
		JavaRDD<String> initialRDD =sc.textFile("C:\\Users\\Mary\\eclipse-workspace\\project_spark\\src\\main\\resources\\data.txt");
		JavaRDD <String> lettersOnlyRDD = initialRDD.map(x-> x.replaceAll("[^a-zA-Z\\ss]", "").toLowerCase());
		JavaRDD <String> removedBlanckLines = lettersOnlyRDD.filter(x->x.trim().length()>0);
		JavaRDD <String> justWords =removedBlanckLines.flatMap(x->Arrays.asList(x.split(" ")).iterator());
		JavaPairRDD <String,Long> pairRDD =justWords.mapToPair(w-> new Tuple2<String,Long>(w,1L));
		JavaPairRDD <String,Long> totals =pairRDD.reduceByKey((value1,value2) -> value1+value2);
		JavaPairRDD <Long,String> switched =totals.mapToPair(tuple ->new Tuple2<Long,String>(tuple._2,tuple._1));
		JavaPairRDD <Long,String> sorted= switched.sortByKey(false);
		List<Tuple2<Long,String>> results =sorted.take(10);
		results.forEach(System.out::println);

		sc.close();		
	}
	
	public void usedDataset() {
		SparkSession sparkSession =SparkSession.builder().config(conf).getOrCreate();
		Dataset<Row>products =sparkSession.read().format("txt").option("header","true").load("C:\\Users\\Mary\\eclipse-workspace\\project_spark\\src\\main\\resources\\data.txt");
		products.show(false);
		
		UDF1<String,String> formatID =new UDF1<String,String>(){
			public String call(String id) {
				return "00"+id;
			}
		};
		sparkSession.udf().register("format_id",formatID, DataTypes.StringType);
		//products =products.withColumn("formatted_id", callUDF("format_id", col("id")));
	
	}		  
		
	
	public static void main(String[] args) throws IOException {
			projectDataframe p = new projectDataframe();
	        p.filterValue();
	        p.usedDataset();
	      	System.out.println("------FINISH------");

	    }
	
	}
