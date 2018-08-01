package SparkTop5ProductsForMonth;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.spark_project.guava.collect.Lists;

import scala.Tuple2;

public final class SparkTop5ProductsForMonthMain {

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		if (args.length < 1) {
			System.err.println("Usage: JavaTop5ProductsForMonth <file>");
			System.exit(1);
		}
		SparkConf conf = new SparkConf().setAppName("JavaTop5ProductsForMonth");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> lines = context.textFile(args[0]);
		JavaPairRDD<String, Integer> yearMonthProdsID_Avg = lines.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String s) {
				String [] tokens = s.split("\t");
				//converts date and sets the string
				Calendar currentTime = Calendar.getInstance();
				Long timestamp = Long.parseLong(tokens[7]);
				currentTime.setTimeInMillis(timestamp * 1000);
				StringBuilder sb = new StringBuilder(); 
				sb.append(currentTime.get(Calendar.YEAR));
				if (currentTime.get(Calendar.MONTH) + 1 < 10) 
					sb.append("0"+(currentTime.get(Calendar.MONTH) + 1)); 
				else 
					sb.append(currentTime.get(Calendar.MONTH) + 1);
				sb.append(" " + tokens[1]); //append Product Id
				String yearMonthProdID = sb.toString();
				return new Tuple2(yearMonthProdID, Integer.parseInt(tokens[6]));
			} 
		});

		JavaPairRDD<String, Tuple2<Integer, Integer>> valueCount = yearMonthProdsID_Avg.mapValues(value -> new Tuple2<Integer, Integer>(value,1));
		//add values by reduceByKey
		JavaPairRDD<String, Tuple2<Integer, Integer>> reducedCount = valueCount.reduceByKey((tuple1,tuple2) ->  new Tuple2<Integer, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
		//calculate average
		JavaPairRDD<String, Double> yearMonthProdID_avg = reducedCount.mapToPair(getAverageByKey);
		//la chiave diventa annoMese avg e valore ProductID
		JavaPairRDD<String, String> yearMonthAvg_IDProd = yearMonthProdID_avg.mapToPair(new PairFunction<Tuple2<String,Double>, String, String>(){

			@Override
			public Tuple2<String, String> call(Tuple2<String, Double> input) throws Exception {
				String [] tokens = input._1.split(" ");
				String key =  tokens[0] +" "+ input._2;
				String value =  tokens[1];
				return new Tuple2(key, value);
			}
		});
		//per stesso annoMese e avg si fa un Iterable di ProductID - lo sort qui è importante se no come risultato vengono le avg mischiate
		//e non le top
		JavaPairRDD<String, Iterable<String>> timeAvg_IDProds =  yearMonthAvg_IDProd.groupByKey().sortByKey();
		//la chiave diventa annoMese e valore diventa Anno, Avg&[ProductID, ProductID,...]
		JavaPairRDD<String, String> yearMonth_avgProdsID = timeAvg_IDProds.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>(){

			@Override
			public Tuple2<String, String> call(Tuple2<String, Iterable<String>> input) throws Exception {
				String [] tokens = input._1.split(" ");
				String key =  tokens[0];
				String value = tokens[1] +"&"+input._2.toString();
				return new Tuple2(key, value);
			}
		});
		
		Map<String, Iterable<String>> map_time_avgsProdsID = yearMonth_avgProdsID.groupByKey().collectAsMap();
		File file = new File("/Users/davinderkumar/Desktop/SparkTop5.txt");
		FileWriter fw = new FileWriter(file);
		PrintWriter pw = new PrintWriter(fw);

		TreeSet <String> setKey = new TreeSet<String> (map_time_avgsProdsID.keySet());
		setKey.forEach(data -> {
			List<String> listAvgsProdsID = Lists.newArrayList(map_time_avgsProdsID.get(data).iterator());
			int size = listAvgsProdsID.size();
			int k = 0;
			while(size > 0 && k < 5){
				String [] s = listAvgsProdsID.get(size -1).split("&");
				//System.out.println(data+ " "+s[0] +" "+s[1]);
				pw.println(data+ " "+s[0] +" "+s[1]+"\n");
				size--;
				k++;
			}
		}); 
		pw.close();

		long endTime = System.currentTimeMillis() - startTime;
		Double time = ((Number)endTime).doubleValue() / 1000;
		System.out.println("\n\nTempo impiegato SparkTop5: "+time);
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