package HadoopGustiAffini;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GustiAffiniMapper2  extends Mapper<Text, Text, Text, Text>{
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = key.toString().split("&");	
		for (int i = 0; i < tokens.length; i++){
			context.write(new Text(tokens[i]), new Text(value));
		}
	}
}
