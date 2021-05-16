// cc Clean Application to do data cleaning
// vv StockCleanMapper
import java.io.IOException;
import java.util.HashMap;
import java.util.Map; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockCleanMapper
  extends Mapper<LongWritable, Text, Text, Text> {
  
  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
    	String line = value.toString();
    	String [] record = line.split(",");

      boolean isValid = true;
      isValid = isValid && (record.length == 5);
	
      String time = "";
      String[] date = record[0].trim().split("/");
      if (date.length == 3) {
      	time = date[2] + "/" + date[0];
      }
      isValid = isValid && (time != "");

      double high = 0;
      double low = 0;
      try {
        high = Double.parseDouble(record[2].trim());
      } catch (NumberFormatException nfe) {
       	isValid = false;
      }
      try {
        low = Double.parseDouble(record[3].trim());
      } catch (NumberFormatException nfe) {
       	isValid = false;
      }
      if (isValid) {
        isValid = isValid && (high > 0) && (low > 0);
      }

      if (isValid) {
	double avgPrice = (high + low) / 2;
    	context.write(new Text(time), new Text(Double.toString(avgPrice)));
      }
  }
}
