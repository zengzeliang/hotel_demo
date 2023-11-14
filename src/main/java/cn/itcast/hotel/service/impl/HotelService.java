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
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

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
    public PageResult hotelFilters(RequestParams requestParams) {
//        SearchRequest searchRequest = new SearchRequest("hotel");
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        if(!StringUtils.isEmpty(requestParams.getKey())){
//            boolQueryBuilder.must(QueryBuilders.matchQuery("all", requestParams.getKey()));
//        }
//        if(!StringUtils.isEmpty(requestParams.getStarName())){
//            boolQueryBuilder.filter(QueryBuilders.termQuery("starName", requestParams.getStarName()));
//        }
//        if(!StringUtils.isEmpty(requestParams.getCity())){
//            boolQueryBuilder.filter(QueryBuilders.termQuery("city", requestParams.getCity()));
//        }
//
//        if(!StringUtils.isEmpty(requestParams.getBrand())){
//            boolQueryBuilder.filter(QueryBuilders.termQuery("brand", requestParams.getBrand()));
//        }
//
//        if(requestParams.getMinPrice() != null){
//            RangeQueryBuilder priceRangeQueryBuilder = QueryBuilders.rangeQuery("price");
//
//            boolQueryBuilder.filter(priceRangeQueryBuilder.gte(requestParams.getMinPrice()));
//            priceRangeQueryBuilder.lte(requestParams.getMaxPrice());
//        }
//
//        searchRequest.source().query(boolQueryBuilder);
//
//        try {
//            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
//
//            return handleResult(response);
//        }catch (Exception e){
//            throw new RuntimeException("ES查询错误");
//        }
        return null;
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
