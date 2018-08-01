package HadoopGustiAffini;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GustiAffiniReducer2 extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> list = new  ArrayList<String>();
		for (Text prodID : values)
			list.add(prodID.toString());
		if(list.size() >= 3){
			context.write(new Text(key), new Text(list.toString()));	
		}
	}
}
