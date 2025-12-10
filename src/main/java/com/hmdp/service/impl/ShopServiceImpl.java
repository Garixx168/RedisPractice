package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

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
        //1.根据id查询redis中是否有店铺信息
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {//这只是查询到商铺数据才为true，null和""都为false
            //2.有，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);//将json转换为Java对象
            return Result.ok(shop);
        }
        //判断命中的是否是空值
        if (shopJson != null){//排除有数据和为null的情况，返回空串的情况
            //返回一个错误信息
            return Result.fail("店铺信息不存在");
        }
        //3。没有，查询数据库
        Shop shop = getById(id);
        //4..数据库没有查到结果，将空值写入redis
        if (shop == null){
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return Result.fail("店铺不存在");
        }
        //5.数据库有结果，写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //返回
        return Result.ok(shop);
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
