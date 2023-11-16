package daframe;

/**
 * Utilizzo di Dataframe
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

public class Example {

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
        public void setScore(String tokens) {
            this.score = tokens;
        }    

        //instance variables
        private String disease;
        private String id;
        private String score;
    }
    
    public static void main(String[] args) throws ClassNotFoundException, AnalysisException {

       SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark");
       JavaSparkContext sc=new JavaSparkContext(sparkConf);
       SQLContext sqlcontext= new SQLContext(sc);
       sc.setLogLevel("ERROR");
       
        SparkSession spark = SparkSession.builder()
    			.master("local[*]")
    	    	.appName("Java Spark SQL Example")
    	    	.getOrCreate();
       // spark.sparkContext().setLogLevel("Error");
        
        //spark.sparkContext().setLogLevel("Error");
              
       String filePath=("C:\\Users\\Mary\\eclipse-workspace\\project_spark\\src\\main\\resources\\dataframe\\data_mal.txt");
                
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
        /**
         * uso di dataframe usando sql context
         */
        Dataset<Row> df = sqlcontext.createDataFrame(schemaRdd, Schema.class);
        df.createOrReplaceTempView("malattie");
     //   df.groupBy("score").count().show();
      
        
      //  System.out.println("numero totale di malattie: "+df.count());
       // df.show();
      //  df.printSchema();
      //  df.select("id").show();
     //   df.select(df.col("disease"), df.col("score")).show();
     //   df.filter("score>100").show();
     //   df.filter(df.col("disease").contains("Carcinoma")).show();
     //   df.where(df.col("disease").startsWith("A")).show();
              
        
       /**
        * uso di dataframe usando SQL 
        */
       Dataset<Row> sqlDF1 = spark.sql("SELECT disease FROM malattie");
      // sqlDF1.show();
       Dataset<Row> sqlDF2 = spark.sql("SELECT id FROM malattie");
       //sqlDF2.show();
       Dataset<Row> sqlDF3 = spark.sql("SELECT * FROM malattie"); 
       //sqlDF3.where("score>100").show();
       System.out.println("mal con score>100: "+sqlDF3.where("score>100").count());        
       Dataset<Row> sqlDF4 = spark.sql("SELECT count(score) FROM malattie");
       Dataset<Row> sqlDF5 = spark.sql("SELECT max(score) FROM malattie");
       Dataset<Row> sqlDF6 = spark.sql("SELECT disease FROM malattie");
       //sqlDF4.show();  
        
        
    }   
}