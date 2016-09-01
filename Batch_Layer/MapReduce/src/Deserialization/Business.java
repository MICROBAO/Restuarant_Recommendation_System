package Deserialization;

import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Business {
	@JsonProperty("type")
	public String type;
	
	@JsonProperty("business_id")
	public String businessId;	
	
	@JsonProperty("name")
	public String name;
	
	@JsonProperty("neighborhoods")
	public List<String>  neighborhood;
	
	@JsonProperty("full_address")
	public String fullAddress;
	
	@JsonProperty("city")
	public String city;
	
	@JsonProperty("state")
	public String state;
	
//	@JsonProperty("latitue")
//	public String latitude;
//	
//	@JsonProperty("longitude")
//	public String longitude;
	
	@JsonProperty("stars")
	public String stars;
	
	@JsonProperty("review_count")
	public String reviewCount;
	
	@JsonProperty("categories")
	public List<String> category;
	
	@JsonProperty("open")
	public boolean open;

}
