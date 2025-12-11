package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import jdk.nashorn.internal.ir.ReturnNode;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.util.retry.Retry;

import javax.annotation.Resource;

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

    /**
     * 根据id查询店铺信息
     * @param id 店铺id
     * @return 店铺信息
     */
    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //queryWithPassThrough(id);
        //缓存穿透
        Shop shop = queryWithMutex(id);
        if (shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

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
    private Shop queryWithPassThrough(Long id) {
        //1.根据id查询redis中是否有店铺信息
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {//这只是查询到商铺数据才为true，null和""都为false
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
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
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
}
