package daframe;
/**
 * Utilizzo di Dataset e Dataframe
 * @author Mary Bonomo
 */
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class DataFrameExample {

    //static schema class
    public static class Schema implements Serializable {

       
        public String getDisease() {
            return disease;
        }
        public void setDisease(String disease) {
            this.disease = disease;
        }
        public String getId() {
            return id;
        }
        public void setId(String id) {
            this.id = id;
        }
        public String getScore() {
            return score;
        }
        public void setScore(String score) {
            this.score = score;
        }    

        //instance variables
        private String disease;
        private String id;
        private String score;
    }
    
    public static void main(String[] args) throws ClassNotFoundException, AnalysisException {

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkDataset");

        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        SparkSession spark = SparkSession.builder()
    			.master("local[2]")
    	    	.appName("Java Spark SQL Example")
    	    	.getOrCreate();
        sc.setLogLevel("ERROR");
        SQLContext sqlcontext= new SQLContext(sc);

        //we have a file which ";" separated 
        String filePath=("C:\\Users\\Mary\\eclipse-workspace\\project_spark\\src\\main\\resources\\dataframe\\data_mal.txt");
        String file2=("C:\\Users\\Mary\\eclipse-workspace\\project_spark\\src\\main\\resources\\dataframe\\\\data2.txt");
        
       
        JavaRDD<Schema> schemaRdd = sc.textFile(filePath).map(
                new Function<String, Schema>() {
                    public Schema call(String line) throws Exception {
                        String[] tokens=line.split(";");
                        Schema schema = new Schema();
                        schema.setId(tokens[0]);
                        schema.setDisease(tokens[1]);
                        schema.setScore(tokens[2]);
                        return schema;
                    }
                });
       
        Dataset<Row> df = sqlcontext.createDataFrame(schemaRdd, Schema.class);
        df.createOrReplaceTempView("malattie");
      
       
       Dataset<Row> sqlDF = spark.sql("SELECT * FROM malattie");      
                     
       /**
        * schema 2 file
        */
        JavaRDD<Schema> schema2 = sc.textFile(file2).map(
                new Function<String, Schema>() {
                    public Schema call(String line) throws Exception {
                        String[] token=line.split(";");
                        Schema schema2 = new Schema();
                        schema2.setId(token[0]);
                        schema2.setDisease(token[1]);
                        schema2.setScore(token[2]);
                        return schema2;
                    }
                });
        
        Dataset<Row> df1 = sqlcontext.createDataFrame(schema2, Schema.class);
        df1.createOrReplaceTempView("malattie_2");
        Dataset<Row> sqlDF1 = spark.sql("SELECT * FROM malattie_2");
        
        /**
         * UNION DISEASE
         */
        //System.out.println("INSIEME MALATTIE");
        //df1.union(df).show();    
        //sqlDF.union(sqlDF1).distinct().show();
        long N_union =sqlDF.union(sqlDF1).count();
       // System.out.println("tutte le malattie: "+N_union);
        /**
         * INTERSECTION DISEASE
         */
       // System.out.println("INTERSEZIONE MALATTIE");
       // sqlDF.intersect(sqlDF1).show();
        long N_inters =sqlDF.intersect(sqlDF1).count();
       // System.out.println("intersezione: "+N_inters);
      
        //USANDO SQL
        Dataset<Row> sqlDF25 = spark.sql("SELECT id, score FROM malattie_2 WHERE score < 100 ORDER BY score");
        sqlDF25.show();
        
        /*
         * altre query di esempio
         */
       //Dataset<Row> query =sqlcontext.createDataFrame(schemaRdd, Schema.class);
        df.createOrReplaceTempView("ASSOCIAZIONI");       
       // df.show();
        df1.select("id","score").filter("score <100").orderBy("score").show();
        
       
        
        
        
        
    }   
}