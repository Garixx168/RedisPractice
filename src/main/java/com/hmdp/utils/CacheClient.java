package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import io.lettuce.core.GeoArgs;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
/**
 * 基于StringRedisTemplate封装一个缓存工具类
 */
public class CacheClient {
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    private final StringRedisTemplate stringRedisTemplate;
    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }
    /**
     * 将任意java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
     */
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**
     * 将任意java对象序列化为json并存储在string类型的key中,并且可以设置逻辑过期时间，用于处理缓存击穿问题
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 根据指定的key获取缓存数据，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
     */
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> Type, Function<ID,R> dbFallback,Long time, TimeUnit unit) {
        //1.根据id查询redis中是否有店铺信息
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {//这只是查询到商铺数据才为true，null和""都为false
            //2.有，直接返回
            R r = JSONUtil.toBean(json, Type);//将json转换为Java对象
            return r;
        }
        //判断命中的是否是空值
        if (json != null){//排除有数据和为null的情况，返回空串的情况
            //返回一个错误信息
            return null;
        }
        //3。没有，查询数据库
        R r = dbFallback.apply(id);
        //4..数据库没有查到结果，将空值写入redis
        if (r == null){
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //5.数据库有结果，写入redis
        this.set(key, r, time, unit);
        //返回
        return r;
    }

    /**
     * 根据指定的key获取缓存数据，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题
     */
    public <R, ID>  R queryWithLogicExpire(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        //1.从redis查询商铺缓存
        String key = keyPrefix + id;
        //1.根据id查询redis中是否有店铺信息
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(json)) {
            //2.未命中，返回空
            return null;
        }
        //3.命中
        //3.1将shopJson反序列化为java对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //3.2判断缓存是否过期
        if (expireTime.isAfter(LocalDateTime.now())){
            //4.未过期，直接返回商铺信息
            return r;
        }
        //5.过期，尝试获取互斥锁，判断是否获得锁//6.没有获取锁，返回过期的商铺信息 ，这个判断可以不用做，后面都会return shop
        String keyLock = LOCK_SHOP_KEY + id;
        boolean tryLock = tryLock(keyLock);
        if (tryLock){
            //7.获取锁，开启独立线程查询数据库
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //8.重建缓存（写入redis）,并设置过期时间
                try {
                    //查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入缓存
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //9.释放锁
                    unLock(keyLock);
                }
            });
        }
        //10.返回数据
        return r;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     */
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}
