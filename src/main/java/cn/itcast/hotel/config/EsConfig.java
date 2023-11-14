package cn.itcast.hotel.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EsConfig {

    @Bean
    public RestHighLevelClient client(){
        return new RestHighLevelClient(RestClient.builder(HttpHost.create("http://127.0.0.1:9200")));
    }
}
