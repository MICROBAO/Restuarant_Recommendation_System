package Deserialization;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)

public class User {
	@JsonProperty("type")
	public String type;
	
	@JsonProperty("user_id")
	public String userId;
	
	@JsonProperty("name")
	public String name;
	
	@JsonProperty("review_count")
	public String reviewCount;
	
	@JsonProperty("average_stars")
	public String averageStars;	
}
