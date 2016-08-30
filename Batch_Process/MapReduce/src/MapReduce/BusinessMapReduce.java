package MapReduce;
import java.io.IOException;
import Parser.*;
import Deserialization.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;


public class BusinessMapReduce {
	public static class JsonMapper extends Mapper<Object, Text, Text, Text> {
		private BusinessWritable info = new BusinessWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				//initialize a ObjectMapper to help read json
				ObjectMapper mapper = new ObjectMapper();
				
				//split data by each line
	            String[] tuple = value.toString().split("\\n");
	            
	            for(int i=0;i<tuple.length; i++) {
	            	//read data based on Business.class in Business.java
	            	Business business = mapper.readValue(tuple[i], Business.class);
	            	 
	            	//the restuarant must be open now and filtered by review count, keep more than 4
	            	if(business.open == true && business.reviewCount != null && Integer.parseInt(business.reviewCount) > 4) {	 
	            		 
	            		//clean raw data by replacing \n with empty string in full address
	            		business.fullAddress = business.fullAddress.replace("\n", "");
	            		info.SetBusiness(business.name);
	            		info.SetStars(business.stars);
	            		 
	            		//Output example --- Restuarant : ABC, Location : ABC, ABC   5
	            		if(business.fullAddress != null && business.city!= null && business.state != null){
	            		     context.write(new Text("Restuarant : " + business.name + ", Location : " + business.fullAddress +", " + business.city +", " + business.state), new Text(business.stars));
	            		}
	            	}
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
