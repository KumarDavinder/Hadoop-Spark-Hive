package HadoopTop10ProductsForUser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10ProductsForUserReducer  extends Reducer<Text, IntWritable, Text, DoubleWritable>{
	private static final TreeMap<String, TreeMap<Double, TreeSet<String>>> mapTop10Products = new TreeMap<String, TreeMap<Double, TreeSet<String>>>();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int counter = 0;
		double sum = 0;
		for (IntWritable value : values) { //average Score
			counter++;
			sum += value.get();
		}			
		double average = Math.floor((sum/counter) * 1000) / 1000;

		String[] tokens = key.toString().split(" ");	
		if (!mapTop10Products.containsKey(tokens[0])){ 
			//se non c'è una TreeMap associata a questa chiave user allora creiamo tutto
			TreeMap<Double, TreeSet<String>> averageProductsID = new TreeMap<Double, TreeSet<String>>();
			TreeSet<String> productsID = new TreeSet<String>();
			productsID.add(tokens[1]);
			averageProductsID.put(new Double(average), productsID);
			mapTop10Products.put(tokens[0], averageProductsID);
		}
		else if(!mapTop10Products.get(tokens[0]).containsKey(average)) { 
			//c'è user ma manca questo average
			TreeSet<String> productsID = new TreeSet<String>();
			productsID.add(tokens[1]);
			mapTop10Products.get(tokens[0]).put(average, productsID);
		}
		else
			mapTop10Products.get(tokens[0]).get(average).add(tokens[1]);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		int counter;
		double avg;
		for (String user : mapTop10Products.keySet()){
			counter = 1;
			TreeMap<Double, TreeSet<String>> avgProducts = mapTop10Products.get(user);
			List <Double> averagesList = new ArrayList<Double>(avgProducts.keySet());
			for (int i = 1; i <= averagesList.size() && counter <= 10; i++) {
				avg = averagesList.get(averagesList.size() - i);
				TreeSet<String> products = avgProducts.get(avg);
				for (String product : products)
					context.write(new Text(user+" "+product), new DoubleWritable(avg));		
				counter++;
			}
		}
	}
}
