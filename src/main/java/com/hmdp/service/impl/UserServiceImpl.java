package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexPatterns;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.quartz.LocalDataSourceJobStore;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)){ //ture:不符合 false:符合
            //2.如果不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }
        //3.符合，生成验证码
        String code = RandomUtil.randomNumbers(6);//生成6位数字验证码
        //4.保存验证码到session
        //session.setAttribute("code",code);
        //4.保存验证码到redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //5.发送验证码
        log.debug("发送短信验证码成功，验证码：{}",code);
        //返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1.校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)){ //ture:不符合 false:符合
            //2.如果不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }
        //2.校验验证码
//      Object cacheCode = session.getAttribute("code");//从session获取验证码
        String code = loginForm.getCode();//用户填写的验证码
//        if (cacheCode == null || !cacheCode.toString().equals(code)){//反向验证可以避免if嵌套
//            return Result.fail("验证码错误！");
//        }
        //从redis获取验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY+phone);
        if (cacheCode == null || !cacheCode.equals(code)){
            return Result.fail("验证码错误！");
        }
        //3.查询用户
        User user = query().eq("phone", phone).one();//使用mp的查询功能
        if (user == null){
            //4.不存在，创建新用户并保存到数据库
            user = createUserWithPhone(phone);
        }
        //5.封装成userdto保存到session
        //session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);

        //保存用户信息到redis中
        //5.1随机生成token，作为登录令牌
        String token = UUID.randomUUID().toString(true);
        //5.2将userDTO转为hashmap存储
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue( true)
                        .setFieldValueEditor((fieldName,fieldValue)->fieldValue.toString()));
        //5.3存储
        String tokenKey = LOGIN_USER_KEY+token;
        stringRedisTemplate.opsForHash().putAll(tokenKey,userMap);
        //5.4设置token有效期
        stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL,TimeUnit.MINUTES);
        //6.返回token
        return Result.ok(token);
    }

    /**
     * 用户签到
     * @return
     */
    @Override
    public Result sign() {
        //1.获取当前用户
        Long usrId = UserHolder.getUser().getId();
        //2.获取当前日期
        LocalDateTime now = LocalDateTime.now();
        String sufFixKey = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        //3.拼接成key
        String key = USER_SIGN_KEY+usrId+sufFixKey;
        //4.获取当天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5.写入redis setbit key offset 1
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth-1,true);
        //6.返回
        return Result.ok();
    }

    /**
     * 统计签到
     * @return
     */
    @Override
    public Result signCount() {
        //1.获取当前用户
        Long usrId = UserHolder.getUser().getId();
        //2.获取当前日期
        LocalDateTime now = LocalDateTime.now();
        String sufFixKey = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        //3.拼接成key
        String key = USER_SIGN_KEY+usrId+sufFixKey;
        //4.获取当天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5.查询redis得到的是一个十进制数
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        if (result == null || result.isEmpty()){
            return Result.ok(0);//没有任何签到结果
        }
        Long num = result.get(0);
        if (num == null){
            return Result.ok(0);
        }
        //6.循环遍历
        int count = 0;
        while (true){
            //6.1将这个十进制数与1进行与运算
            if ((num & 1) == 0){
                //6.4为0，结束循环
                break;
            }else {
                //6.3为1，count+1
                count++;
            }
            num >>>= 1;//右移一位，抛弃最后一个bit位，继续下一个bit位
        }
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX+RandomUtil.randomString(10));
        //调用mp的保存功能保存用户到数据库
        save(user);
        return user;
    }
}
