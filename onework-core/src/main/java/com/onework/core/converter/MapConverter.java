package com.onework.core.converter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.util.Map;

/**
 * @author kangj
 * @date 2020/11/20
 **/
@Converter
public class MapConverter implements AttributeConverter<Map<String, String>, String> {
    @Override
    public String convertToDatabaseColumn(Map<String, String> map) {
        return JSON.toJSONString(map);
    }

    @Override
    public Map<String, String> convertToEntityAttribute(String s) {
        return JSON.parseObject(s, new TypeReference<Map<String, String>>() {
        });
    }
}
