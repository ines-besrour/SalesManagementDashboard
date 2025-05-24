package spark.kafka;


import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import scala.Tuple2;

import static org.apache.spark.sql.functions.col;


public class SparkKafkaCustomers {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: KafkaConsumer <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("KafkaConsumer")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topics)
                .option("kafka.group.id", groupId)
                .load();

        Dataset<Row> lines = df.selectExpr("CAST(value AS STRING)");

        Dataset<Row> processedData = lines.selectExpr("split(value, ',') as data")
                .filter(col("data").getItem(0).rlike("\\d+")) // Filtre pour un entier
                .selectExpr("cast(data[0] as int) as customer_id", "data[10] as source") // Accès à la colonne Source avec l'index 10
                .filter(col("source").rlike("\\w+")); // Filtre pour des valeurs non vides dans la colonne Source

        Dataset<Row> customersPerSource = processedData.groupBy("source").count();

        StreamingQuery query = customersPerSource.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        query.awaitTermination();
    }
}
