package HadoopGustiAffini;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GustiAffiniMain {

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		Job job1 = new Job(new Configuration(), "GustiAffini - step1");
		Job job2 = new Job(new Configuration(), "GustiAffini - step2");

		job1.setJarByClass(GustiAffiniMain.class);
		job1.setMapperClass(GustiAffiniMapper1.class);
		job1.setReducerClass(GustiAffiniReducer1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);	
		
		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, GustiAffiniMapper1.class);
		Path path = new Path("/gustiAffini");
		FileOutputFormat.setOutputPath(job1, path);
		job1.waitForCompletion(true);
		
		FileInputFormat.setInputPaths(job2, path);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		 //Di default è LongWritable se non va scritto questo lui tenta un cast Text, LongWritable -> errore
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setJarByClass(GustiAffiniMain.class);
		job2.setMapperClass(GustiAffiniMapper2.class);
		job2.setReducerClass(GustiAffiniReducer2.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.waitForCompletion(true);
		
		long endTime = System.currentTimeMillis() - startTime;
		Double time = ((Number)endTime).doubleValue() / 1000;
		System.out.println("\n\nTempo impiegato HadoopGustiAffini: "+time);
	}

}
