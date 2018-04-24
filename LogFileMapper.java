/**
 * Heather Nolis
 * Data Science
 * SeattleU
 * HW1
 */

package solution;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class LogFileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  //convert line to a string
	  String line = value.toString();
	  
	  //split the line at the first space to give IP address and the rest
	  String lineParts[] = line.split(" ", 2);
	  
	  //save the IP address
	  String ipAddress = lineParts[0].toString();
	  
	  //count the ip address if it exists
	  if(ipAddress != null && ipAddress.length() > 0){
		  
		  //store key-value pair IPADDRESS,1 (since this is one occurrence)
		  context.write(new Text(ipAddress), new IntWritable(1));
	  }

  }
}