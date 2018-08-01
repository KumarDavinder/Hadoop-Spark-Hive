package HadoopTop5ProductsForMonth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Top5ProductsReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	private static final TreeMap<String, TreeMap<Double, ArrayList<byte[]>>> mapTop5Products = new TreeMap<String, TreeMap<Double, ArrayList<byte[]>>>();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int counter = 0;
		double sum = 0;
		for (IntWritable value : values) { //average Score
			counter++;
			sum += value.get();
		}
		double average = Math.floor((sum/counter) * 1000) / 1000;
		
		String[] tokens = key.toString().split(" ");	
		if (!mapTop5Products.containsKey(tokens[0])){ 
			//se non c'è una TreeMap associata a questa chiave allora creiamo tutto
			TreeMap<Double, ArrayList<byte[]>> averageProductsID = new TreeMap<Double, ArrayList<byte[]>>();
			ArrayList<byte []> productsID = new ArrayList<byte []>();
			productsID.add(tokens[1].getBytes());
			averageProductsID.put(new Double(average), productsID);
			mapTop5Products.put(tokens[0], averageProductsID);
		}
		else if(!mapTop5Products.get(tokens[0]).containsKey(average)) { 
			//c'è mese e anno ma manca questo average
			ArrayList<byte []> productsID = new ArrayList<byte []>();
			productsID.add(tokens[1].getBytes());
			mapTop5Products.get(tokens[0]).put(average, productsID);
		}
		else 
			mapTop5Products.get(tokens[0]).get(average).add(tokens[1].getBytes());
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		 int counter;
		 double avg;
		 for (String monthYear : mapTop5Products.keySet()){
             counter = 1;
             TreeMap<Double, ArrayList<byte[]>> avgProductsID = mapTop5Products.get(monthYear);
             List <Double> averagesList = new ArrayList<Double>(avgProductsID.keySet());
            // Collections.sort(averagesList, Collections.reverseOrder()); //for top 5
        	 for (int i = 1; i <= averagesList.size() && counter <= 5; i++) {
 				 avg = averagesList.get(averagesList.size() - i);
 				ArrayList<byte[]> products = avgProductsID.get(avg);
        		 for (byte[] product : products)
        			context.write(new Text(monthYear+" "+new String(product, "UTF-8")), new DoubleWritable(avg));		
    			 counter++;
             }
         }
	}
}