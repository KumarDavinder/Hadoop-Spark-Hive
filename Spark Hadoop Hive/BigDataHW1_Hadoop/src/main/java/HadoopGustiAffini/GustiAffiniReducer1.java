package HadoopGustiAffini;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GustiAffiniReducer1 extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		TreeSet<String> set = new  TreeSet<String>();
		for (Text UID1 : values)
			set.add(UID1.toString());
		List<String> list = new ArrayList<String>(set);
		StringBuilder sb = new StringBuilder();
		int i = 0;
		int j = 0;
		while (i < list.size()){
			j = i+1;
			while(j < list.size()){
				sb.append(list.get(i)+","+list.get(j)+"&");
				j++;
			}
			i++;
		}
		if(j > 1)
			context.write(new Text(sb.toString()), new Text(key));	
	}
}
