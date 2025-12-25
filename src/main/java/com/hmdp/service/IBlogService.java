package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IBlogService extends IService<Blog> {

    /**
     * 查询热门博客
     * @param current 当前页码
     * @return 热门博客
     */
    Result queryHotBlog(Integer current);

    /**
     * 根据博客id查询博客详情
     * @param id 博客id
     * @return 博客详情
     */
    Result queryBlogById(Long id);

    /**
     * 点赞博客
     * @param id 博客id
     */
    void blogLike(Long id);
}
