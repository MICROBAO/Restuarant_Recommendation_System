import twitter4j.conf.ConfigurationBuilder;

public class ConfigurationProvider {

    public static ConfigurationBuilder getConfig(){
        String custkey, custsecret;
        String accesstoken, accesssecret;
        custkey = "gaBGaSGVi5wmzYfPMYhcfWbXn";
        custsecret="ycRP6RzzWGtShVQEX9ESmGH5rw3mZgRs2D6fivZR7Q4Fmc5qBG";
        accesstoken ="739682825863995393-MYm1MYMg19agr9XB4c4mK1nNIoIRSdM";
        accesssecret ="pILCis6bkXoAwHevwaD3oFCVXP2AEC5k5UFNHvXUX1M0C"; 

        ConfigurationBuilder config =
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(custkey)
                        .setOAuthConsumerSecret(custsecret)
                        .setOAuthAccessToken(accesstoken)
                        .setOAuthAccessTokenSecret(accesssecret) ;
        return config;
    }
}
