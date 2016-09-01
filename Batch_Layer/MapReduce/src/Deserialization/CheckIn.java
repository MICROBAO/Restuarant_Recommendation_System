package Deserialization;
import java.util.HashMap;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
@JsonIgnoreProperties(ignoreUnknown = true)

public class CheckIn {
	@JsonProperty("type")
	public String type;
	
	@JsonProperty("business_id")
	public String businessId;
	
	@JsonProperty("checkin_info")
	public HashMap<String, Integer> checkInfo;
	
}
