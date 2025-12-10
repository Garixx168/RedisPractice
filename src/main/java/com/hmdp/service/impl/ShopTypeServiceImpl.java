package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    /**
     * 查询所有商铺类型
     * @return 商铺类型列表
     */

    @Override
    public List<ShopType> queryList() {
        //1.从redis中查询商户类型列表
        String key =CACHE_SHOP_TYPE_KEY;
        String typeJson = stringRedisTemplate.opsForValue().get(key);//从redis中查询到的是json类型的数据
        //2.查到,直接返回
        if (StrUtil.isNotBlank(typeJson)){
            return JSONUtil.toList(typeJson, ShopType.class);
        }
        //3.没查到，查询数据库
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        //4.没查到，返回一个空集合
        if (shopTypeList.isEmpty()){
            return Collections.emptyList();//返回一个空集合
        }
        //5.查到，返回数据并写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shopTypeList));
        //6.返回
        return shopTypeList;
    }
}
