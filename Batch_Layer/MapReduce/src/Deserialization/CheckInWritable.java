package Deserialization;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CheckInWritable implements Writable{
	private String businessId;
	private String SundayCheckNum;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		businessId = in.readUTF();  
		SundayCheckNum =  in.readUTF();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(businessId);
		out.writeUTF(SundayCheckNum);
	}
	public void SetBusinessId(String businessId) {
		this.businessId = businessId;
	}
	
	public void SetSundayCheckNum(String SundayCheckNum) {
		this.SundayCheckNum = SundayCheckNum;
	}
	
	public String GetBusinessId() {
		return businessId;
	}
	
	public String GetSundayCheckNum() {
		return SundayCheckNum;
	}
	
}
