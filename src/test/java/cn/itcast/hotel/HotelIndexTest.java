package cn.itcast.hotel;

import cn.itcast.hotel.constants.HotelConstants;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;


@SpringBootTest
class HotelIndexTest {
    private RestHighLevelClient client;

    @Test
    void testCreateIndex() throws IOException{
        CreateIndexRequest request = new CreateIndexRequest("hotel");

        request.source(HotelConstants.HOTEL_MAPPING, XContentType.JSON);

        client.indices().create(request, RequestOptions.DEFAULT);
    }


    @Test
    void testExistsIndex() throws IOException{
        GetIndexRequest request = new GetIndexRequest("hotel");

        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);

        System.out.println("判断是否存在: " + exists);
    }

    @Test
    void testDeleteIndex() throws IOException{
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        client.indices().delete(request, RequestOptions.DEFAULT);

    }


    @BeforeEach
    void setUp(){
        this.client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create("http://127.0.0.1:9200"))
        );
    }

    @AfterEach
    void endUp() throws IOException {
        this.client.close();
    }

}
