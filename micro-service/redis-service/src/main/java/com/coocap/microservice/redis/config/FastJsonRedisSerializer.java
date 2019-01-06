package com.coocap.microservice.redis.config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

import java.io.UnsupportedEncodingException;

public class FastJsonRedisSerializer<T> implements RedisSerializer<T> {

    public static final String DEFAULT_CHARSET = "UTF-8";

    private Class<T> clazz;

    public FastJsonRedisSerializer(Class<T> clazz) {
        super();
        this.clazz = clazz;
    }

    @Override
    public byte[] serialize(T t) throws SerializationException {

        if (null == t){
            return new byte[0];
        }

        try {
            return JSON.toJSONString(t, SerializerFeature.WriteClassName).getBytes(DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return new byte[0];
        }

    }

    @Override
    public T deserialize(byte[] bytes) throws SerializationException {

        if (null == bytes || bytes.length <= 0){
            return null;
        }

        try {
            String str = new String(bytes, DEFAULT_CHARSET);
            return JSON.parseObject(str, clazz);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }

    }
}
