package Deserialization;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


public class ReviewWritable implements Writable {

	private String text;
	private String stars;
	
	public void readFields(DataInput in) throws IOException {
		text = in.readUTF();  
		stars =  in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(text);
		out.writeUTF(stars);
	}

	public void SetReview(String name) {
		this.text = name;
	}
	
	public void SetStar(String stars) {
		this.stars = stars;
	}
	
	public String GetReview() {
		return text;
	}
	
	public String GetStar() {
		return stars;
	}

}


