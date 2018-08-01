package HadoopTop5ProductsForMonth;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Top5ProductsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String [] tokens = value.toString().split("\t");	
		//converts date and sets the string
		Calendar currentTime = Calendar.getInstance();
		Long timestamp = Long.parseLong(tokens[7]);
		currentTime.setTimeInMillis(timestamp * 1000);
		
		StringBuilder sb = new StringBuilder(); 
		sb.append(currentTime.get(Calendar.YEAR));
		int month = currentTime.get(Calendar.MONTH);
		if ( month + 1 < 10) 
			sb.append("0"+(month + 1)); 
		else 
			sb.append(month + 1);
		sb.append(" " + tokens[1]); //append Product Id
		String yearMonthProdID = sb.toString();
		context.write(new Text(yearMonthProdID), new IntWritable(Integer.parseInt(tokens[6])));
	}
}