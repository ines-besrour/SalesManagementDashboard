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
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BestSeller {
    private static final Logger LOGGER = LoggerFactory.getLogger(BestSeller.class);

    public static void main(String[] args) {
        Preconditions.checkArgument(args.length > 2, "Please provide the paths of input files and the output file as parameters.");
        new BestSeller().run(args[0], args[1], args[2], args[3]);
    }

    public void run(String customersFilePath, String productsFilePath, String ordersFilePath, String outputFilePath) {
        String master = "local[*]";
        SparkConf conf = new SparkConf()
                .setAppName(BestSeller.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load customers data and calculate the age of each customer
        JavaRDD<String> customersLines = sc.textFile(customersFilePath);
        JavaPairRDD<Integer, Long> customerAges = customersLines.mapToPair(line -> {
            String[] parts = line.split(",");
            LocalDate birthDate = LocalDate.parse(parts[3]);
            int age = LocalDate.now().getYear() - birthDate.getYear();
            return new Tuple2<>(age, 1L);
        }).reduceByKey((a, b) -> a + b);

        // Load orders data
        JavaRDD<String> ordersLines = sc.textFile(ordersFilePath);

        // Map each order to a tuple of (productID, quantity) and then reduce to get total quantity for each product
        JavaRDD<String> ordersDataWithoutHeader = ordersLines.filter(line -> !line.startsWith("ID")); // Skip header row

        JavaPairRDD<String, Integer> productQuantities = ordersDataWithoutHeader.mapToPair(line -> {
            String[] parts = line.split(",");
            String productId = parts[3];
            int quantity = Integer.parseInt(parts[4]);
            return new Tuple2<>(productId, quantity);
        }).reduceByKey((a, b) -> a + b);

        // Load products data
        JavaRDD<String> productsLines = sc.textFile(productsFilePath);

        // Map each product to a tuple of (productID, category)
        JavaPairRDD<String, String> productCategories = productsLines.mapToPair(line -> {
            String[] parts = line.split(",");
            String productId = parts[0];
            String category = parts[1];
            return new Tuple2<>(productId, category);
        });

        // Join productQuantities with productCategories to get (productID, (quantity, category))
        JavaPairRDD<String, Tuple2<Integer, String>> productQuantitiesWithCategory = productQuantities.join(productCategories);

        // Group productQuantitiesWithCategory by age and find the most purchased product for each age group
        JavaPairRDD<Integer, Tuple2<String, Integer>> mostPurchasedProductByAge = productQuantitiesWithCategory.groupByKey().flatMapToPair(ageProducts -> {
            int age = Integer.parseInt(ageProducts._1());
            Iterable<Tuple2<Integer, String>> products = ageProducts._2();
            Map<String, Integer> productCountMap = new HashMap<>();
            for (Tuple2<Integer, String> product : products) {
                String productId = product._2();
                int quantity = product._1();
                productCountMap.put(productId, productCountMap.getOrDefault(productId, 0) + quantity);
            }
            String mostPurchasedProductId = Collections.max(productCountMap.entrySet(), Map.Entry.comparingByValue()).getKey();
            return Collections.singletonList(new Tuple2<>(age, new Tuple2<>(mostPurchasedProductId, productCountMap.get(mostPurchasedProductId)))).iterator();
        });

        // Collect results as a map for easier output handling
        Map<Integer, Tuple2<String, Integer>> mostPurchasedProductByAgeMap = mostPurchasedProductByAge.collectAsMap();

        // Write the results to the output file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            for (Map.Entry<Integer, Tuple2<String, Integer>> entry : mostPurchasedProductByAgeMap.entrySet()) {
                int age = entry.getKey();
                Tuple2<String, Integer> productCount = entry.getValue();
                writer.write("Age: " + age + ", Most Purchased Product ID: " + productCount._1() + ", Quantity: " + productCount._2() + "\n");
            }
        } catch (IOException e) {
            LOGGER.error("Error writing to output file: {}", e.getMessage());
        }

        // Stop Spark context
        sc.stop();
    }
}
