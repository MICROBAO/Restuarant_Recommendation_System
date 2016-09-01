package MapReduce;
import Parser.*;
import Deserialization.*;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

public class UserMapReduce {
    //Mapper function
	public static class JsonMapper extends Mapper<Object, Text, Text, Text> {

		private UserWritable info = new UserWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				ObjectMapper mapper = new ObjectMapper();
	            String[] tuple = value.toString().split("\\n");
	             for(int i=0;i<tuple.length; i++) {
	            	 User user = mapper.readValue(tuple[i], User.class);
	            	 
	            	 //filter when review count is less than 3
	            	 if(user.reviewCount != null && Integer.parseInt(user.reviewCount) > 3) {
	            		 info.SetName(user.name);
	            		 info.SetStars(user.averageStars);
	            		 if(user.name != null && user.averageStars != null){
	            			 //output name and average star of the users
	            		     context.write(new Text(user.name), new Text(user.averageStars));
	            		 }
	            	 }
	              }
			} catch (JSONException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	          }
		}
	}
	
	//main function
    public static void main(String[] args) throws Exception {
   	 runJob(args[0], args[1]);
   }
   
   public static void runJob(String input, String output) throws Exception {
       Configuration conf = new Configuration();
       Job job = new Job(conf);
       job.setJarByClass(BusinessMapReduce.class);
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
