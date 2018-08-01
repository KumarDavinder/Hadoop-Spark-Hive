package SparkTop10ProductsForUser;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.spark_project.guava.collect.Lists;

import scala.Tuple2;

public class SparkTop10ProductsForUser {
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		if (args.length < 1) {
			System.err.println("Usage: JavaTop10ProductsForUser <file>");
			System.exit(1);
		}
		SparkConf conf = new SparkConf().setAppName("JavaTop10ProductsForUser");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> lines = context.textFile(args[0]);

		//UserID ProdId, Score
		JavaPairRDD<String, Integer> userProdID_score = lines.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String s) {
				String[] tokens = s.split("\t");	
				StringBuilder sb = new StringBuilder(); 
				sb.append(tokens[2]); //UserID
				sb.append(" " + tokens[1]); //ProductID
				String userIDProdID = sb.toString();
				return new Tuple2(userIDProdID, Integer.parseInt(tokens[6])); //userIDProdID, Score
			} 
		});

		JavaPairRDD<String, Tuple2<Integer, Integer>> valueCount = userProdID_score.mapValues(value -> new Tuple2<Integer, Integer>(value,1));
		//add values by reduceByKey
		JavaPairRDD<String, Tuple2<Integer, Integer>> reducedCount = valueCount.reduceByKey((tuple1,tuple2) ->  new Tuple2<Integer, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
		//calculate average
		JavaPairRDD<String, Double> userProdID_avg = reducedCount.mapToPair(getAverageByKey);
		//la chiave diventa UserID avg e valore ProductID
		JavaPairRDD<String, String> userIDAvg_IDProd = userProdID_avg.mapToPair(new PairFunction<Tuple2<String,Double>, String, String>(){

			@Override
			public Tuple2<String, String> call(Tuple2<String, Double> input) throws Exception {
				String [] tokens = input._1.split(" ");
				String key =  tokens[0] +" "+ input._2;
				String value =  tokens[1];
				return new Tuple2(key, value);
			}
		});
		//per stesso annoMese e avg si fa un Iterable di ProductID - lo sort qui è importante se no 
		//come risultato vengono le avg mischiate e non le top
		JavaPairRDD<String, Iterable<String>> userIDAvg_IDProds =  userIDAvg_IDProd.groupByKey().sortByKey();
		//la chiave diventa annoMese e valore diventa Avg&[ProductID, ProductID,...]
		JavaPairRDD<String, String> userID_avgProdsID = userIDAvg_IDProds.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>(){

			@Override
			public Tuple2<String, String> call(Tuple2<String, Iterable<String>> input) throws Exception {
				String [] tokens = input._1.split(" ");
				String key =  tokens[0];
				String value = tokens[1] +"&"+input._2.toString();
				return new Tuple2(key, value);
			}
		});

		//JavaPairRDD<String, Iterable<String>> a = userID_avgProdsID.groupByKey();
		//		a.foreach(data -> {
		//			System.out.println(data._1() +" "+data._2().toString());
		//		});
		Map<String, Iterable<String>> yearMonth_avgsProdsID = userID_avgProdsID.groupByKey().collectAsMap();
		TreeSet <String> yearsMonths = new TreeSet<String> (yearMonth_avgsProdsID.keySet());
		File outputFile = new File("/Users/davinderkumar/Desktop/SparkTop10.txt");
		FileWriter fw = new FileWriter(outputFile);
		PrintWriter pw = new PrintWriter(fw);
		yearsMonths.forEach(data -> {
			List<String> listAvgsProdsID = Lists.newArrayList(yearMonth_avgsProdsID.get(data).iterator());
			int size = listAvgsProdsID.size();
			int k = 0;
			while(size > 0 && k < 10){
				String [] s = listAvgsProdsID.get(size -1).split("&");
				pw.println(data+ " "+s[0] +" "+s[1]);
				size--;
				k++;
			}
		}); 
		pw.close();
		long endTime = System.currentTimeMillis() - startTime;
		Double time = ((Number)endTime).doubleValue() / 1000;
		System.out.println("\n\nTempo impiegato SparkTop10: "+time);
	}

	private static PairFunction<Tuple2<String, Tuple2<Integer, Integer>>,String,Double> getAverageByKey = (tuple) -> {
		Tuple2<Integer, Integer> val = tuple._2;
		double total = val._1;
		int count = val._2;
		double average = Math.floor((total/count) * 1000) / 1000;
		Tuple2<String, Double> averagePair = new Tuple2<String, Double>(tuple._1, average);
		return averagePair;
	};
}
