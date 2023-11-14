package cn.itcast.hotel;

import cn.itcast.hotel.consyants.HotelConstants;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@SpringBootTest
class HotelDocumentTest {
    private RestHighLevelClient client;

    @Autowired
    private IHotelService hotelService;

    @Test
    void testAddDocument() throws IOException{
        // 根据id查询酒店数据
        Hotel hotel =hotelService.getById(61083L);

        HotelDoc hotelDoc = new HotelDoc(hotel);

        IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());

        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);

        client.index(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteDocument() throws IOException{
        // 根据id更新酒店数据
        UpdateRequest request = new UpdateRequest("hotel", "61083");

        request.doc("price", 984, "starName", "四钻");

        client.update(request, RequestOptions.DEFAULT);
    }

    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        List<Hotel> hotels = hotelService.list();
        for(Hotel hotel: hotels){
            HotelDoc hotelDoc = new HotelDoc(hotel);
            bulkRequest.add(
                    new IndexRequest("hotel").id(hotelDoc.getId().toString()).source(JSON.toJSONString(hotelDoc), XContentType.JSON)
            );
        }

        Map<String, String> maps = new HashMap<>();
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    @Test
    void testUpdateDocument() throws IOException{
        // 根据id删除酒店数据
        DeleteRequest request = new DeleteRequest("hotel").id(String.valueOf(61083L));
        client.delete(request, RequestOptions.DEFAULT);
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
