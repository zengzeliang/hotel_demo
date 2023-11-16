package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult hotelList(RequestParams requestParams) {
        SearchRequest searchRequest = new SearchRequest("hotel");
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        buildBasicQuery(requestParams, boolQueryBuilder);

        int page = requestParams.getPage();
        int size = requestParams.getSize();

        searchRequest.source().from((page - 1) * size);
        searchRequest.source().size(size);

        if(!StringUtils.isEmpty(requestParams.getLocation())){
            searchRequest.source().sort(SortBuilders
                    .geoDistanceSort("location", new GeoPoint(requestParams.getLocation()))
                    .order(SortOrder.ASC)
                    .unit(DistanceUnit.KILOMETERS));
        }

        FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders.functionScoreQuery(boolQueryBuilder, new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("beAd", true),
                        ScoreFunctionBuilders.weightFactorFunction(10)
                )
        });
        searchRequest.source().query(functionScoreQueryBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

            return handleResult(response);
        }catch (Exception e){
            throw new RuntimeException("ES查询错误");
        }
    }

    private void buildBasicQuery(RequestParams requestParams, BoolQueryBuilder boolQueryBuilder) {
        if(!StringUtils.isEmpty(requestParams.getKey())){
            boolQueryBuilder.must(QueryBuilders.matchQuery("all", requestParams.getKey()));
        }else{
            boolQueryBuilder.must(QueryBuilders.matchAllQuery());
        }
        if(!StringUtils.isEmpty(requestParams.getStarName())){
            boolQueryBuilder.filter(QueryBuilders.termQuery("starName", requestParams.getStarName()));
        }
        if(!StringUtils.isEmpty(requestParams.getCity())){
            boolQueryBuilder.filter(QueryBuilders.termQuery("city", requestParams.getCity()));
        }

        if(!StringUtils.isEmpty(requestParams.getBrand())){
            boolQueryBuilder.filter(QueryBuilders.termQuery("brand", requestParams.getBrand()));
        }

        if(requestParams.getMinPrice() != null && requestParams.getMaxPrice() != null){
            RangeQueryBuilder priceRangeQueryBuilder = QueryBuilders.rangeQuery("price");

            boolQueryBuilder.filter(priceRangeQueryBuilder.gte(requestParams.getMinPrice()));
            priceRangeQueryBuilder.lte(requestParams.getMaxPrice());
        }
    }

    @Override
    public Map<String, List<String>> hotelFilters(RequestParams requestParams) {
        try {
            SearchRequest searchRequest = new SearchRequest("hotel");
            searchRequest.source().size(0);
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            buildBasicQuery(requestParams, boolQueryBuilder);
            searchRequest.source().query(boolQueryBuilder);


            TermsAggregationBuilder brandAggregationBuilder = AggregationBuilders.terms("brandAgg").field("brand").size(100);
            TermsAggregationBuilder cityAggregationBuilder = AggregationBuilders.terms("cityAgg").field("city").size(100);
            TermsAggregationBuilder starNameAggregationBuilder = AggregationBuilders.terms("starNameAgg").field("starName").size(100);
            searchRequest.source().aggregation(brandAggregationBuilder);
            searchRequest.source().aggregation(cityAggregationBuilder);
            searchRequest.source().aggregation(starNameAggregationBuilder);

            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

            Map<String, List<String>> result = new HashMap<>();
            Aggregations aggregations = response.getAggregations();

            processAggregation("brand", aggregations, result);

            processAggregation("city", aggregations, result);

            processAggregation("starName", aggregations, result);

            return result;
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * @Author zengzeliang
     * @Description //自动补全
     * @Date 2023/11/15 2:38 下午
     * @Param [key]
     * @return java.util.List<java.lang.String>
     **/
    @Override
    public List<String> hotelSuggestion(String key) {

        SearchRequest searchRequest = new SearchRequest("hotel");

        searchRequest.source().suggest(new SuggestBuilder().addSuggestion(
                "hotelSuggest", new CompletionSuggestionBuilder("suggestion").prefix(key).skipDuplicates(true).size(10)));
        List<String> result = new ArrayList<>();
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

            CompletionSuggestion hotelSuggest = response.getSuggest().getSuggestion("hotelSuggest");
            List<CompletionSuggestion.Entry.Option> options = hotelSuggest.getOptions();

            for (CompletionSuggestion.Entry.Option option : options) {
                String opt = option.getText().string();
                result.add(opt);
            }
            return result;
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private void processAggregation(String name, Aggregations aggregations, Map<String, List<String>> result) {
        Terms brandTerms = aggregations.get(name + "Agg");
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        List<String> lists = new ArrayList<>();

        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            lists.add(key);
        }
        result.put(name, lists);

    }


    private PageResult handleResult(SearchResponse response) {

        long total = response.getHits().getTotalHits().value;

        SearchHit[] hits = response.getHits().getHits();
        List<HotelDoc> hotelList = new ArrayList<>();
        for(SearchHit hit : hits){
            String source = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(source, HotelDoc.class);
            hotelDoc.setIsAD(hotelDoc.getBeAd());

            Object[] sortValues = hit.getSortValues();

            if(sortValues.length > 0){
                hotelDoc.setDistance(sortValues[0]);
            }

            hotelList.add(hotelDoc);
        }
        return new PageResult(total, hotelList);
    }
}
