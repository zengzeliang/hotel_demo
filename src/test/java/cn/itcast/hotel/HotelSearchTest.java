package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;


@SpringBootTest
class HotelSearchTest {
    private RestHighLevelClient client;

    @Test
    void testSearchMatchAll() throws IOException{

        SearchRequest request = new SearchRequest("hotel");

        request.source().query(QueryBuilders.matchAllQuery());

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleEsResult(response);

    }

    @Test
    void testSearchHighLight() throws IOException{
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchQuery("all", "如家"));

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field nameField = new HighlightBuilder.Field("name");
        nameField.requireFieldMatch(false);
        nameField.preTags("<span>");
        nameField.postTags("</span>");

        HighlightBuilder.Field brandField = new HighlightBuilder.Field("brand");
        brandField.requireFieldMatch(false);
        brandField.preTags("<em>");
        brandField.postTags("</em>");
        highlightBuilder.field(brandField);
        highlightBuilder.field(nameField);
        request.source().highlighter(highlightBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
//        handleEsResult(response);
        System.out.println(response);
    }

    private void handleEsResult(SearchResponse response) {
        long value = response.getHits().getTotalHits().value;

        System.out.println("共获取：" + value + "条数据");

        SearchHit[] hits = response.getHits().getHits();

        for (SearchHit hit : hits) {
            String source = hit.getSourceAsString();

            HotelDoc hotelDoc = JSON.parseObject(source, HotelDoc.class);

            System.out.println(hotelDoc);
        }
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
