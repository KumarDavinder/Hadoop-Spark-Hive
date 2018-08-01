package it.uniroma3.kucha;

import java.util.Calendar;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Unix2Date extends UDF{

	public Text evaluate(Text text) {
		if(text == null) return null;
		long timestamp = Long.parseLong(text.toString());
		Calendar mydate = Calendar.getInstance();
		mydate.setTimeInMillis(timestamp*1000);
		StringBuilder sb = new StringBuilder(); 
		sb.append(mydate.get(Calendar.YEAR));
		if (mydate.get(Calendar.MONTH)+1<10)
			sb.append("0"+(mydate.get(Calendar.MONTH)+1)); 
		else 
			sb.append(mydate.get(Calendar.MONTH)+1);
		
		String id = sb.toString();
		return new Text(id);
	}


}
