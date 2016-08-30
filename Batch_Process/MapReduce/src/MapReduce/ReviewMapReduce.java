package MapReduce;
import Deserialization.*;
import Parser.*;
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

public class ReviewMapReduce {
	
	public static class JsonMapper extends Mapper<Object, Text, Text, Text> {
		
		private ReviewWritable info = new ReviewWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				//initialize a ObjectMapper to help read json
				ObjectMapper mapper = new ObjectMapper();
				//split data by each line
	            String[] tuple = value.toString().split("\\n");
	            
	            for(int i=0;i<tuple.length; i++) {
	            	//read data corresponding to format in Review.class in Review.java
	            	Review review = mapper.readValue(tuple[i], Review.class);
	            	if(review != null && review.text != null) {
	            		//clean raw data by replacing \n with empty string in comments
	            		review.text = review.text.replace("\n", "");
	            		
	            		info.SetReview(review.text);
	            		info.SetStar(review.stars);
	            		//output comments and star
	            		context.write(new Text(info.GetReview()), new Text(info.GetStar()));
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
        job.setJarByClass(ReviewMapReduce.class);
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
    
    
    
