package HadoopGustiAffini;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GustiAffiniMapper1 extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");	
		if (Integer.parseInt(tokens[6]) >= 4){
			context.write(new Text(tokens[1]), new Text(tokens[2])); //ProductID UserID
		}
	}
}
