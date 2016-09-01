package Deserialization;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class BusinessWritable implements Writable{
	private String business;
	private String stars;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		business = in.readUTF();  
		stars =  in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(business);
		out.writeUTF(stars);
	}
	public void SetBusiness(String business) {
		this.business = business;
	}
	
	public void SetStars(String stars) {
		this.stars = stars;
	}
	
	public String GetBusiness() {
		return business;
	}
	
	public String GetStars() {
		return stars;
	}
	
}
