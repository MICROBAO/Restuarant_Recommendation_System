package Deserialization;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Review {
	@JsonProperty("type")
	public String type;
	
	@JsonProperty("user_id")
	public String userId;
	
	@JsonProperty("stars")
	public String stars;
	
	@JsonProperty("text")
	public String text;
	
//	@JsonProperty("votes")
//	public Vote votes;
//	
//	public class Vote{
//		@JsonProperty("funny")
//		public String funny;
//		
//		@JsonProperty("useful")
//		public String useful;
//		
//		@JsonProperty("cool")
//		public String cool;
//	}	
}

