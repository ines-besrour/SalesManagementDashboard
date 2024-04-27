package spark.customers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

class CitiesCounter {
    private static final Logger LOGGER = LoggerFactory.getLogger(CitiesCounter.class);

    public static void main(String[] args) {
        Preconditions.checkArgument(args.length > 1, "Please provide the path of input file and output file as parameters.");
        new CitiesCounter().run(args[0], args[1]);
    }

    public void run(String inputFilePath, String outputFilePath) {
        String master = "local[*]";
        SparkConf conf = new SparkConf()
                .setAppName(CitiesCounter.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the data file into an RDD
        JavaRDD<String> lines = sc.textFile(inputFilePath);
        System.out.println("hello");
        JavaPairRDD<String, Integer> cityCounts = lines.mapToPair(line -> {
            String[] parts = line.split(",");
            String city = parts[4];
            return new Tuple2<>(city, 1);
        }).reduceByKey((a, b) -> a + b);

        // Convert the RDD to a Map for easier output handling
        Map<String, Integer> cityCountsMap = cityCounts.collectAsMap();

        // Write the count to the output file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            for (Map.Entry<String, Integer> entry : cityCountsMap.entrySet()) {
                writer.write(entry.getKey() + ": " + entry.getValue() + "\n");
            }
        } catch (IOException e) {
            LOGGER.error("Error writing to output file: {}", e.getMessage());
        }

        // Stop Spark context
        sc.stop();
    }
}
