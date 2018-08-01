package it.uniroma3.kucha;

import org.apache.hadoop.hive.ql.exec.UDF;

public final class Rank extends UDF{
	private int  counter;
	private String last_date;
	private String last_avg;
	public int evaluate(String date,String avg){
		if (!date.equalsIgnoreCase(this.last_date)){
			counter = 0;
			this.last_date=date;
			this.last_avg=avg;
		}
		else
			if (!avg.equalsIgnoreCase(this.last_avg)){
				this.last_avg=avg;
				counter++;}

		return counter;
	}
}
