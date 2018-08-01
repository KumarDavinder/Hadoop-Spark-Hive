package HadoopTop10ProductsForUser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top10ProductsForUserMain {


	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		Job job = new Job(new Configuration(), "Top10ProductsForUserMain");
		job.setJarByClass(Top10ProductsForUserMain.class);
		job.setMapperClass(Top10ProductsForUserMapper.class);
		job.setReducerClass(Top10ProductsForUserReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.waitForCompletion(true);
		
		long endTime = System.currentTimeMillis() - startTime;
		Double time = ((Number)endTime).doubleValue() / 1000;
		System.out.println("\n\nTempo impiegato HadoopTop10: "+time);
	}
}