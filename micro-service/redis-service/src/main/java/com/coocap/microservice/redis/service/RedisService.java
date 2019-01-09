package com.coocap.microservice.redis.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * redis缓存服务
 */
@Service
public class RedisService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private ValueOperations<String, Object> valueOperations;

    @Autowired
    private HashOperations<String, String, Object> hashOperations;

    @Autowired
    private ListOperations<String, Object> listOperations;

    @Autowired
    private SetOperations<String, Object> setOperations;

    @Autowired
    private ZSetOperations<String, Object> zSetOperations;

    //---------------------------------------------------------------------
    // key 操作
    //---------------------------------------------------------------------

    /**
     * 删除给定的 key
     * @param keys
     */
    public void del(String... keys){
        redisTemplate.delete(Arrays.asList(keys));
    }

    /**
     * 检查给定 key 是否存在
     * @param key
     */
    public boolean exists(String key){
        return redisTemplate.hasKey(key);
    }

    /**
     * 设置过期
     * @param key
     * @param timeout
     * @param timeUnit
     * @return
     */
    public boolean expire(String key, long timeout, TimeUnit timeUnit){
        return redisTemplate.expire(key, timeout, timeUnit);
    }

    /**
     * 设置某个时间过期
     * @param key
     * @param date
     * @return
     */
    public boolean expireAt(String key, Date date){
        return redisTemplate.expireAt(key, date);
    }

    /**
     * 获取所有key
     * @param pattern
     * @return
     */
    public Set<String> keys(String pattern){
        return redisTemplate.keys(pattern);
    }

    /**
     * 设置key永不过期
     * @param key
     * @return
     */
    public boolean persist(String key){
        return redisTemplate.persist(key);
    }

    /**
     * 随机返回一个key
     * @return
     */
    public String randomkey(){
        return redisTemplate.randomKey();
    }

    /**
     * 重命名一个key
     * @param key
     * @param newKey
     */
    public void rename(String key, String newKey){
        redisTemplate.renameIfAbsent(key, newKey);
    }

    /**
     * 重命名一个key
     * 当且仅当 newkey 不存在时，将 key 改名为 newkey 。
     * @param key
     * @param newKey
     */
    public void renameNx(String key, String newKey){
        redisTemplate.renameIfAbsent(key, newKey);
    }

    /**
     * 获取过期时间
     * @param key
     * @param timeUnit
     * @return
     */
    public long ttl(String key, TimeUnit timeUnit){
        return redisTemplate.getExpire(key, timeUnit);
    }

    /**
     * 获取 key 所储存的值的类型
     * @param key
     * @return
     */
    public DataType type(String key){
        return redisTemplate.type(key);
    }


    //---------------------------------------------------------------------
    // string 操作
    //---------------------------------------------------------------------

    /**
     * 如果 key 已经存在并且是一个字符串， APPEND 命令将 value 追加到 key 原来的值的末尾。
     * 如果 key 不存在， APPEND 就简单地将给定 key 设为 value ，就像执行 SET key value 一样。
     * @param key
     * @param append
     * @return 追加 value 之后， key 中字符串的长度。
     */
    public Integer append(String key, String append){
        return valueOperations.append(key, append);
    }

    /**
     * 将 key 中储存的数字值减一。
     * @param key
     * @return 减一之后的值
     */
    public long decr(String key){
        return valueOperations.decrement(key);
    }

    /**
     * 将 key 所储存的值减去减量 decrement 。
     * 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECRBY 操作。
     * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
     * @param key
     * @param decrment
     * @return 减去 decrement 之后， key 的值。
     */
    public long decrBy(String key, long decrment){
        return valueOperations.decrement(key, decrment);
    }

    /**
     * 获取key的值
     * @param key
     * @return
     */
    public Object get(String key){
        return valueOperations.get(key);
    }

    /**
     * 获取 key 中字符串值的子字符串
     * 字符串的截取范围由 start 和 end 两个偏移量决定(包括 start 和 end 在内)。
     * @param key
     * @return 截取得出的子字符串
     */
    public Object getRange(String key, long start, long end){
        return valueOperations.get(key, start, end);
    }

    /**
     * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)
     * @param key
     * @param value
     * @return key的旧值
     */
    public Object getset(String key, String value){
        return valueOperations.getAndSet(key, value);
    }

    /**
     * 将 key 所储存的值加一
     * @param key
     * @return 加一之后 key 的值。
     */
    public long incr(String key){
        return valueOperations.increment(key);
    }

    /**
     * 将 key 所储存的值减去减量 decrement 。
     * 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECRBY 操作。
     * @param key
     * @param incrment
     * @return 加上 increment 之后， key 的值。
     */
    public long incrBy(String key, long incrment){
        return valueOperations.increment(key, incrment);
    }

    /**
     * 为 key 中所储存的值加上浮点数增量 increment 。
     * @param key
     * @param incrment
     * @return 执行命令之后 key 的值。
     */
    public double incrByFloat(String key, double incrment){
        return valueOperations.increment(key, incrment);
    }

    /**
     * 获取所有(一个或多个)给定 key 的值。
     * @param keyList
     * @return
     */
    public List<Object> mget(List<String> keyList){
        return valueOperations.multiGet(keyList);
    }

    /**
     * 同时设置一个或多个 key-value 对。
     * @param map
     * @return
     */
    public void mset(Map<String, String> map){
        valueOperations.multiSet(map);
    }

    /**
     * 同时设置一个或多个 key-value 对，当且仅当所有给定 key 都不存在。
     * 即使只有一个给定 key 已存在， MSETNX 也会拒绝执行所有给定 key 的设置操作。
     * @param map
     * @return
     */
    public boolean msetNx(Map<String, String> map){
        return valueOperations.multiSetIfAbsent(map);
    }

    /**
     * 设置键值
     * @param key
     * @param value
     */
    public void set(String key, String value){
        valueOperations.set(key, value);
    }

    /**
     * 设置键值并设置过期时间（原子操作）
     * @param key
     * @param value
     * @param timeout
     * @param timeUnit
     */
    public void setEx(String key, String value, long timeout, TimeUnit timeUnit){
        valueOperations.set(key, value, timeout, timeUnit);
    }

    /**
     * 将 key 的值设为 value ，当且仅当 key 不存在。
     * 若给定的 key 已经存在，则 SETNX 不做任何动作。
     * @param key
     * @param value
     * @param timeout
     * @param timeUnit
     * @return
     */
    public boolean setNx(String key, String value, long timeout, TimeUnit timeUnit){
        return valueOperations.setIfAbsent(key, value, timeout, timeUnit);
    }


    //---------------------------------------------------------------------
    // hash 操作
    //---------------------------------------------------------------------

    /**
     * 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略
     * @param key
     * @param field
     * @return
     */
    public long hdel(String key, String... field){
        return hashOperations.delete(key, field);
    }

    /**
     * 查看哈希表 key 中，给定域 field 是否存在。
     * @param key
     * @param field
     * @return
     */
    public boolean hexists(String key, String field){
        return hashOperations.hasKey(key, field);
    }

    /**
     * 返回哈希表 key 中给定域 field 的值。
     * @param key
     * @param field
     * @return
     */
    public Object hget(String key, String field){
        return hashOperations.get(key, field);
    }

    /**
     * 返回哈希表 key 中，所有的域和值。
     * @param key
     * @return
     */
    public Map<String, Object> hgetAll(String key){
        return hashOperations.entries(key);
    }

    /**
     * 为哈希表 key 中的域 field 的值加上增量 increment 。
     * 增量也可以为负数，相当于对给定域进行减法操作。
     * 如果 key 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。
     * 如果域 field 不存在，那么在执行命令前，域的值被初始化为 0 。
     * 对一个储存字符串值的域 field 执行 HINCRBY 命令将造成一个错误。
     * @param key
     * @param field
     * @param increment
     * @return 执行 HINCRBY 命令之后，哈希表 key 中域 field 的值。
     */
    public long hincrBy(String key, String field, long increment){
        return hashOperations.increment(key, field, increment);
    }

    /**
     * 为哈希表 key 中的域 field 加上浮点数增量 increment 。
     * 如果哈希表中没有域 field ，那么 HINCRBYFLOAT 会先将域 field 的值设为 0 ，然后再执行加法操作。
     * 如果键 key 不存在，那么 HINCRBYFLOAT 会先创建一个哈希表，再创建域 field ，最后再执行加法操作。
     * @param key
     * @param field
     * @param increment
     * @return 执行加法操作之后 field 域的值。
     */
    public double hincrByFloat(String key, String field, double increment){
        return hashOperations.increment(key, field, increment);
    }

    /**
     * 返回哈希表 key 中的所有域。
     * @param key
     * @return
     */
    public Set<String> hkeys(String key){
        return hashOperations.keys(key);
    }

    /**
     * 返回哈希表 key 中域的数量。
     * @param key
     * @return
     */
    public long hlen(String key){
        return hashOperations.size(key);
    }

    /**
     * 返回哈希表 key 中，一个或多个给定域的值。
     * @param key
     * @param field
     * @return
     */
    public List<Object> hmGet(String key, String... field){
        return hashOperations.multiGet(key, Arrays.asList(field));
    }

    /**
     * 同时将多个 field-value (域-值)对设置到哈希表 key 中。
     * @param key
     * @param map
     */
    public void hmSet(String key, Map<String, Object> map){
        hashOperations.putAll(key, map);
    }

    /**
     * 将哈希表 key 中的域 field 的值设为 value 。
     * @param key
     * @param field
     * @param value
     */
    public void hset(String key, String field, Object value){
        hashOperations.put(key, field, value);
    }

    /**
     * 将哈希表 key 中的域 field 的值设置为 value ，当且仅当域 field 不存在。
     * @param key
     * @param field
     * @param value
     */
    public void hsetNx(String key, String field, Object value){
        hashOperations.putIfAbsent(key, field, value);
    }

    /**
     * 返回哈希表 key 中所有域的值。
     * @param key
     */
    public List<Object> hvals(String key){
        return hashOperations.values(key);
    }

    /**
     * 返回哈希表 key 中， 与给定域 field 相关联的值的字符串长度（string length）。
     * @param key
     * @param field
     * @return
     */
    public long hstrlen(String key, String field){
        return hashOperations.lengthOfValue(key, field);
    }


    //---------------------------------------------------------------------
    // list 操作
    //---------------------------------------------------------------------

    /**
     * 阻塞式弹出列表头元素
     * @param key
     * @param timeout
     * @param timeUnit
     * @return
     */
    public Object blpop(String key, long timeout, TimeUnit timeUnit){
        return listOperations.leftPop(key, timeout, timeUnit);
    }

    /**
     * 阻塞式弹出列表尾元素
     * @param key
     * @param timeout
     * @param timeUnit
     * @return
     */
    public Object brpop(String key, long timeout, TimeUnit timeUnit){
        return listOperations.rightPop(key, timeout, timeUnit);
    }

    /**
     * 阻塞式
     * 将列表 source 中的最后一个元素(尾元素)弹出，并返回给客户端。
     * 将 source 弹出的元素插入到列表 destination ，作为 destination 列表的的头元素。
     * @param source
     * @param destination
     * @param timeout
     * @param timeUnit
     * @return
     */
    public Object brpopLpush(String source, String destination, long timeout, TimeUnit timeUnit){
        return listOperations.rightPopAndLeftPush(source, destination, timeout, timeUnit);
    }

    /**
     * 获取列表 key 中，下标为 index 的元素。
     * @param key
     * @param index
     * @return
     */
    public Object lindex(String key, long index){
        return listOperations.index(key, index);
    }

    /**
     * 获取列表 key 的长度。
     * @param key
     * @return
     */
    public long llen(String key){
        return listOperations.size(key);
    }

    /**
     * 移除并返回列表 key 的头元素。
     * @param key
     * @return
     */
    public Object lpop(String key){
        return listOperations.leftPop(key);
    }

    /**
     * 将一个或多个值 value 插入到列表 key 的表头
     * @param key
     * @return
     */
    public long lpush(String key, List<Object> list){
        return listOperations.leftPushAll(key, list);
    }

    /**
     * 将值 value 插入到列表 key 的表头，当且仅当 key 存在并且是一个列表。
     * @param key
     * @return
     */
    public long lpushx(String key, Object value){
        return listOperations.leftPushIfPresent(key, value);
    }

    /**
     * 返回列表 key 中指定区间内的元素，区间以偏移量 start 和 stop 指定。
     * @param key
     * @return
     */
    public List<Object> lrange(String key, long start, long stop){
        return listOperations.range(key, start, stop);
    }

    /**
     * 根据参数 count 的值，移除列表中与参数 value 相等的元素。
     * @param key
     * @return 被移除元素的数量。
     */
    public long lrem(String key, long count, Object value){
        return listOperations.remove(key, count, value);
    }

    /**
     * 将列表 key 下标为 index 的元素的值设置为 value 。
     * @param key
     * @param index
     * @param value
     */
    public void lset(String key, long index, Object value){
        listOperations.set(key, index, value);
    }

    /**
     * 对一个列表进行修剪(trim)，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
     * @param key
     * @param start
     * @param stop
     */
    public void ltrim(String key, long start, long stop){
        listOperations.trim(key, start, stop);
    }

    /**
     * 移除并返回列表 key 的尾元素。
     * @param key
     * @return
     */
    public Object rpop(String key){
        return listOperations.rightPop(key);
    }

    /**
     * 非阻塞式
     * 将列表 source 中的最后一个元素(尾元素)弹出，并返回给客户端。
     * 将 source 弹出的元素插入到列表 destination ，作为 destination 列表的的头元素。
     * @param source
     * @param destination
     * @return
     */
    public Object rpopLpush(String source, String destination){
        return listOperations.rightPopAndLeftPush(source, destination);
    }

    /**
     * 将一个或多个值 value 插入到列表 key 的表尾(最右边)。
     * @param key
     * @return
     */
    public long rpush(String key, List<Object> list){
        return listOperations.rightPushAll(key, list);
    }

    /**
     * 将值 value 插入到列表 key 的表尾，当且仅当 key 存在并且是一个列表。
     * @param key
     * @return
     */
    public long rpushx(String key, Object value){
        return listOperations.rightPushIfPresent(key, value);
    }





}
