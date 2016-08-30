package MapReduce;
import java.io.IOException;
import java.util.Map;

import MapReduce.BusinessMapReduce.JsonMapper;
import Parser.*;
import Deserialization.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

public class CheckInMapReduce {
	public static class JsonMapper extends Mapper<Object, Text, Text, Text> {
		private CheckInWritable info = new CheckInWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				//initialize a ObjectMapper to help read json
				ObjectMapper mapper = new ObjectMapper();
				
				//split data by each line
	            String[] tuple = value.toString().split("\\n");
	            
	            for(int i=0;i<tuple.length; i++) {
	            	//read data based on Business.class in Business.java
	            	CheckIn checkIn = mapper.readValue(tuple[i], CheckIn.class);
	            	int count = 0;
	            	if(checkIn.businessId != null &&  checkIn.checkInfo != null) {	 
	            		Map<String, Integer> map = checkIn.checkInfo;
	            		
	            		for(String check : map.keySet()) {
	            			if(check.charAt(check.length() - 1) == '0') {
	            				count = count + map.get(check);
	            			}
	            		}
	            		info.SetBusinessId(checkIn.businessId);
	            		info.SetSundayCheckNum(Integer.toString(count));
	            	}
	            	context.write(new Text(checkIn.businessId), new Text(Integer.toString(count)));
	            	
	            }
			} catch (JSONException e) {
				//print error msg when exceptions are thrown
	            e.printStackTrace();
	          }
		}
	}
    public static void main(String[] args) throws Exception {
     	 runJob(args[0], args[1]);
   }	
   public static void runJob(String input, String output) throws Exception {
	   Configuration conf = new Configuration();
       Job job = new Job(conf);
       job.setJarByClass(CheckInMapReduce.class);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(Text.class);
       job.setMapperClass(JsonMapper.class);
       job.setOutputFormatClass(TextOutputFormat.class);
       FileInputFormat.setInputPaths(job, new Path(input));
       Path outPath = new Path(output);
       FileOutputFormat.setOutputPath(job, outPath);
       outPath.getFileSystem(conf).delete(outPath, true);
       job.waitForCompletion(true);
    }
}
