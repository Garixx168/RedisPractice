package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import jdk.nashorn.internal.ir.ReturnNode;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.util.retry.Retry;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static java.lang.Thread.sleep;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;
    /**
     * 根据id查询店铺信息
     * @param id 店铺id
     * @return 店铺信息
     */
    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //缓存击穿
        //Shop shop = queryWithMutex(id);
        Shop shop = cacheClient.queryWithLogicExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //
        if (shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    //定义一个线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    /**
     * 逻辑过期解决缓存击穿
     */
/*
    private Shop queryWithLogicExpire(Long id) {


        //1.从redis查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        //1.根据id查询redis中是否有店铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(shopJson)) {
            //2.未命中，返回空
            return null;
        }
        //3.命中
        //3.1将shopJson反序列化为java对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //3.2判断缓存是否过期
        if (expireTime.isAfter(LocalDateTime.now())){
            //4.未过期，直接返回商铺信息
            return shop;
        }
        //5.过期，尝试获取互斥锁，判断是否获得锁//6.没有获取锁，返回过期的商铺信息 ，这个判断可以不用做，后面都会return shop
        String keyLock = LOCK_SHOP_KEY + id;
        boolean tryLock = tryLock(keyLock);

        if (tryLock){
            //7.获取锁，开启独立线程查询数据库
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //8.重建缓存（写入redis）,并设置过期时间
                try {
                    this.saveShopToRedis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //9.释放锁
                    unLock(keyLock);
                }
            });
        }
        //10.返回数据
        return shop;
    }
*/
    /**
     * 互斥锁解决缓存击穿
     */
    private Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        String keyLock = LOCK_SHOP_KEY + id;
        Shop shop = null;
        //1.根据id查询redis中是否有店铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {//这只是查询到商铺数据才为true，null和""都为false
            //2.有，直接返回
            shop = JSONUtil.toBean(shopJson, Shop.class);//将json转换为Java对象
            return shop;
        }
        //判断命中的是否是空值
        if (shopJson != null){//排除有数据和为null的情况，返回空串的情况
            //返回一个错误信息
            return null;
        }
        try {
            //3.1未命中，尝试获取互斥锁
            boolean Lock = tryLock(keyLock);
            //3.2没有获取到，说明已经有线程获取锁在查询数据库，休眠一段时间重试查询redis缓存
            if (!Lock){
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //3.3获取锁成功，再次检测redis缓存是否存在，如果存在则无需重建缓存
            shopJson = stringRedisTemplate.opsForValue().get(key);
            //3.4存在，返回
            if (StrUtil.isNotBlank(shopJson)) {//这只是查询到商铺数据才为true，null和""都为false
                //有，直接返回
               shop = JSONUtil.toBean(shopJson, Shop.class);//将json转换为Java对象
                return shop;
            }
            //判断命中的是否是空值
            if (shopJson != null){//排除有数据和为null的情况，返回空串的情况
                //返回一个错误信息
                return null;
            }
            //3.5不存在，查询数据库，再重建缓存
           shop = getById(id);
            //4..数据库没有查到结果，将空值写入redis
            if (shop == null){
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //5.数据库有结果，写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //6.释放锁
            unLock(keyLock);
        }
        //返回
        return shop;
    }

    /**
     * 缓存穿透
     */
/*    private Shop queryWithPassThrough(Long id) {
        //1.根据id查询redis中是否有店铺信息
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {//这只是查询到商铺数据才为true，null和“”都为false
            //2.有，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);//将json转换为Java对象
            return shop;
        }
        //判断命中的是否是空值
        if (shopJson != null){//排除有数据和为null的情况，返回空串的情况
            //返回一个错误信息
            return null;
        }
        //3。没有，查询数据库
        Shop shop = getById(id);
        //4..数据库没有查到结果，将空值写入redis
        if (shop == null){
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //5.数据库有结果，写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //返回
        return shop;
    }*/

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

    /**
     * 更新店铺信息
     * @param shop
     * @return
     */
    @Transactional
    @Override
    public Result update(Shop shop) {
        Long shopId = shop.getId();
        if (shopId == null){
            return Result.fail("店铺id不能为空");
        }
        String key = CACHE_SHOP_KEY + shopId;
        //1.先操作数据库
        updateById(shop);
        //2.再删除缓存
        stringRedisTemplate.delete(key);
        return Result.ok();
    }

    /**
     * 保存店铺信息到redis(缓存预热，模拟提前将热点信息存入redis)
     * @param id
     * @param expireSeconds
     */
    public void saveShopToRedis(Long id, Long expireSeconds) throws InterruptedException {
        //1.查询店铺数据
        Shop shop = getById(id);
        //睡眠200ms模拟数据库耗时，耗时越长，出现线程安全问题的概率就越大
        Thread.sleep(200);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
}
