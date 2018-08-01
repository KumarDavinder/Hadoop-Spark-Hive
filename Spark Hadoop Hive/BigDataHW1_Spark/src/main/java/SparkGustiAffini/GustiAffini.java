package SparkGustiAffini;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.tools.nsc.ast.parser.Tokens;

public class GustiAffini {
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		if (args.length < 1) {
			System.err.println("Usage: GustiAffini <file>");
			System.exit(1);
		}
		SparkConf conf = new SparkConf().setAppName("GustiAffini");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> lines = context.textFile(args[0]);

		JavaRDD<String> firstMap = lines.filter(s -> {
			String [] tokens = s.toString().split("\t");
			int score = Integer.parseInt(tokens[6]);
			if(score >= 4)
				return true;
			return false;
		});
		
		JavaPairRDD<String, String> prodUserID = firstMap.mapToPair(new PairFunction<String, String, String>() { 
			//prende come input Str s e produce output Str Str
			@Override
			public Tuple2<String, String> call(String s) {
				String line = s.toString();
				String[] tokens = line.split("\t");	
				return new Tuple2(tokens[1], tokens[2]); //ProdID, UserID
			} 
		});

		prodUserID.join(prodUserID)
		.filter(data -> data._2._1.compareTo(data._2._2) <0)
		.mapToPair(data -> new Tuple2<>(data._2._1 + " " + data._2._2 , data._1))
		.groupByKey()
				.filter(data ->{
					int i = 0;
					for (String w :  data._2){
						i++;
						if (i == 3){
							return true;
						}
					}
					return false;
				})
			.sortByKey()
		.saveAsTextFile("/Users/davinderkumar/Desktop/GustiAffini.txt");

		long endTime = System.currentTimeMillis() - startTime;
		Double time = ((Number)endTime).doubleValue() / 1000;
		System.out.println("\n\nTempo impiegato GustiAffini Spark: "+time);
	} 
}

