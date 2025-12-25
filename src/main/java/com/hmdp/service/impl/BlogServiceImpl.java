package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        //1.查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在！");
        }
        //2.查询blog有关的用户
        queryBlogUser(blog);
        //3.查询blog是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        //获取登录用户
        Long userId = UserHolder.getUser().getId();
        //判断当前用户是否点过赞
        String key = "blog:liked:"+blog.getId();
        Boolean isMember = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        blog.setIsLike(BooleanUtil.isTrue(isMember));
    }


    @Override
    public void blogLike(Long id) {
        //获取用户id
        Long userId = UserHolder.getUser().getId();

        //判断是否点过赞,就是判断该用户有没有在set集合中，set集合，key为blogId,value为用户id
        String key = "blog:liked:"+id;
        Boolean isMember = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        if (BooleanUtil.isFalse(isMember)){ //为什么不能直接写isMember,而要写BooleanUtil.isFalse(isMember) 原因：Boolean为包装类，而boolean为基本数据类型，两者不一致
            //没点过，点赞，修改数据库并将用户id存入set集合
            boolean isSuccesss = update().setSql("liked=liked+1").eq("id", id).update();
            if (isSuccesss){
                stringRedisTemplate.opsForSet().add(key,userId.toString());
            }
        }else {
            //存在，也就是点过，点过，则取消点赞，修改数据库并从set中移除该用户id
            boolean isSuccess = update().setSql("liked = liked -1").eq("id", id).update();
            if (isSuccess){
                stringRedisTemplate.opsForSet().remove(key,userId.toString());
            }

        }
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        com.hmdp.entity.User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
