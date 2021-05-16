// cc Clean Application to do data cleaning
// vv HousingCleanMapper
import java.io.IOException;
import java.util.HashMap;
import java.util.Map; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HousingCleanMapper
  extends Mapper<LongWritable, Text, Text, Text> {
  
  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
      String line = value.toString();
      String [] record = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

      if (record.length == 21) {
	boolean isValid = true;

	String saleDate = record[20].replace("\"", "").replace("2016", "16").trim();
	String [] saleTime = saleDate.split("/");
	String year = "";
	String month = "";
	
	isValid = (saleTime.length == 3) && isValid;
	if (isValid) {
		year = saleTime[2];
		month = saleTime[0];
		if (month.length() == 1) {
			month = "0" + month;
		}
	}

	// Only consider class1 or class2 property
	String taxClass = record[17].replace("\"", "").trim();
	isValid = isValid && (taxClass.equals("1") || taxClass.equals("2"));
	
	String salePrice = "";
	int salePriceValue = 0;
	String [] salePrices = record[19].replace("\"", "").replace("$", "").replace(".00", "").trim().split(",");
   	for(int i = 0; i < salePrices.length; i++) {
		salePrice += salePrices[i];
      	}
	try {
   		salePriceValue = Integer.parseInt(salePrice);
    	} catch (NumberFormatException nfe) {}
	isValid = isValid && (salePriceValue > 100);

	String grossArea = "";
	int grossAreaValue = 0;
	String [] grossAreas = record[15].replace("\"", "").trim().split(",");
   	for(int i = 0; i < grossAreas.length; i++) {
		grossArea += grossAreas[i];
      	}
	try {
   		grossAreaValue = Integer.parseInt(grossArea);
    	} catch (NumberFormatException nfe) {}
	isValid = isValid && (grossAreaValue > 1);

	if (isValid) {
        	context.write(new Text(year + "/" + month), new Text(salePrice + "," + grossArea));
	}
      }	
   } 
}
