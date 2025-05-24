package spark.streaming.project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;
import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.printf;

public class Stream {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException  {
        SparkSession spark = SparkSession
                .builder()
                .appName("Streaming")
                .master("local[*]")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<String> lines = spark
                .readStream()
                .format("socket")
                .option("host", "172.25.3.84")
                .option("port", 9999)
                .load()
                .as(Encoders.STRING());

        System.out.println("col extraction");

    // Parse the CSV-like format directly to extract the customer ID and source columns
        Dataset<Row> df = lines.selectExpr("split(value, ',') as data")
                .filter(col("data").getItem(0).rlike("\\d+")) // Filtre pour un entier
                .selectExpr("cast(data[0] as int) as customer_id", "data[10] as source") // Accès à la colonne Source avec l'index 10
                .filter(col("source").rlike("\\w+")); // Filtre pour des valeurs non vides dans la colonne Source



// Group by 'source' and count the number of customers per source
        Dataset<Row> customersPerSource = df.groupBy("source").count();


        // Start running the query that prints the running counts to the console
        StreamingQuery query = customersPerSource.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();


        query.awaitTermination();
    }
}
