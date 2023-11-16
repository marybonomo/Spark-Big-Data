/**
 * author: Mary Bonomo
 * PROGRAMMA: utilizzo di java RDD in spark
 * dati due file contenenti le malattie restituire un file contenente le malattie distinte 
 * ... controllare che le malattie di entrambi i file siano distinte e inserirle in un file. 
 */
package project_spark;

//LIBRERIE DA IMPORTARE

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


public class projectRDD {
		//PER ACCEDERE AL CLUSTER BISOGNA CREARE UN CONTESTO
		SparkConf conf = new SparkConf().setAppName("projectRDD").setMaster("local[*]");
		private JavaSparkContext sc = new JavaSparkContext(conf);
		
		///dichiarazione RDD
		private JavaRDD<String> data;// file malattie - id
		private JavaRDD<String> data2; // file mirna - malattie
		
		private JavaRDD<String> malattie_union;
		private JavaRDD<String> malattie_inters;
		private JavaRDD<String> disease;
		private JavaRDD <String> filtro;
		
		public projectRDD() {
	        this.sc.setLogLevel("ERROR");
	    }
		
		public void readFiles() {
			
	        data = sc.textFile("src/main/resources/data.txt");
	        data2 = sc.textFile("src/main/resources/data_mal.txt");
	    }
		
		private void filterValues() throws IOException {
	       System.out.println("************START FILTER VALUE**************\n");			
	       FileWriter writer = new FileWriter("src/main/resources/intersection.txt");
	       
			JavaRDD<String> col1 = data.map(x -> x.split("	")[0]);//malattia
	        JavaRDD<String> col2 = data.map(x -> x.split("	")[1]);//id
	        System.out.println(col2.distinct().count());

	        //secondo file che contiene gene - MALATTIA
	        JavaRDD<String> col1a = data2.map(x -> x.split(",")[0]);//gene
	        JavaRDD<String> col2a = data2.map(x -> x.split(",")[1]);//malattia
	        
	        //System.out.println(col1.collect());
	        System.out.println("malattie "+col1.count());
	        JavaRDD <String> distinto=col1.distinct();
	        distinto.collect();
	        //System.out.println(col2a.collect());
	      //  System.out.println("malattie data2: "+col2a.count());
	        
	      //  System.out.println("malattie distinte "+col1.distinct().count());
	        
	        
	        JavaRDD <String> unione=col1.union(col2a);
	       
	        System.out.println("unione malattie\n");
	        System.out.println(unione.collect());
	        Long conteggio= unione.count();
	        System.out.println(" unione conteggio: "+unione.distinct().count());
	        
	        JavaRDD <String>intersect=col1.intersection(col2a);
	        System.out.println(intersect.collect());
	        System.out.println(intersect.count());
	        
	        
	        this.malattie_union=col1.distinct().union(col2a.distinct());//unione
	        System.out.println("unione distinta:"+malattie_union.count());
	        this.malattie_inters=col1.intersection(col2a);
	        long intersection=col1.intersection(col2a).count();
	        
	        System.out.println("MALATTIE IN COMUNE");
	      
	        this.malattie_inters.collect().forEach(association -> {
                String malattia = association.split(",")[0];
                System.out.println(malattia);
            });
	        
	      
		}
		/*
		private void setValues() {
			System.out.println("*******START SET VALUE***********\n");	
			this.disease=data.map(x -> x.split("	")[0]);
			//data2.map(x -> x.split(";")[1]);
			System.out.println("malattie primo file\n");
			System.out.println(disease.collect());
			
			
			System.out.println("malattie secondo file");
			data2.collect().forEach(association -> {
	            String geni = association.split(";")[0];
	            String malattie = association.split(";")[1];	            
	            JavaRDD <String> filtro =data.filter(couple -> malattie.contains(couple.split("	")[0]));
	            
	            //System.out.println(filtro.collect());
	       
						
			 });
			
		}
		*/
		
		public static void main(String[] args) throws IOException {
	        projectRDD p = new projectRDD();
	        p.readFiles();
	        p.filterValues();
	        //p.setValues();
	        System.out.println("------FINISH------");

	    }
		
	
	      
}
	
