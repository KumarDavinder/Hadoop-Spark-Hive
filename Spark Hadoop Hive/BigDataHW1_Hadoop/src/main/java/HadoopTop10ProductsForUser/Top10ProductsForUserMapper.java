package HadoopTop10ProductsForUser;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10ProductsForUserMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");	
		StringBuilder sb = new StringBuilder(); 
		sb.append(tokens[2]); //append User ID
		sb.append(" " + tokens[1]); //append Product ID
		String userIDProdID = sb.toString();
		context.write(new Text(userIDProdID), new IntWritable(Integer.parseInt(tokens[6])));
	}

}
