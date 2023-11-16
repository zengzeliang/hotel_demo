package cn.itcast.hotel.controller;

import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/hotel")
public class HotelController {
    @Autowired
    private IHotelService hotelService;

    @PostMapping("/list")
    public PageResult hotelList(@RequestBody RequestParams requestParams){
        return hotelService.hotelList(requestParams);
    }

    @PostMapping("/filters")
    public Map<String, List<String>> hotelFilters(@RequestBody RequestParams requestParams){
        return hotelService.hotelFilters(requestParams);
    }

    @GetMapping("/suggestion")
    public List<String> hotelSuggestion(String key){
        return hotelService.hotelSuggestion(key);
    }

}
