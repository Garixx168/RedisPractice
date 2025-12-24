package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import io.lettuce.core.RedisCommandInterruptedException;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Autowired
    private SeckillVoucherServiceImpl seckillVoucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    //定义阻塞队列
    //private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    //创建线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    //定义脚本、加载lua脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    /*@PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }*/
    //线程销毁
    @PreDestroy
    public void destroy() {
        // 关闭线程池
        SECKILL_ORDER_EXECUTOR.shutdown();
        try {
            // 等待现有任务完成
            if (!SECKILL_ORDER_EXECUTOR.awaitTermination(10, TimeUnit.SECONDS)) {
                // 强制关闭
                SECKILL_ORDER_EXECUTOR.shutdownNow();
            }
        } catch (InterruptedException e) {
            SECKILL_ORDER_EXECUTOR.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }


    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取队列中的信息 xreadgroup group g1 c1 count 1 block 2000 stream stream.order
                    List<MapRecord<String, Object,Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2.判断消息获取是否成功
                    if (list == null || list.isEmpty()){
                        continue;
                    }
                    //3.解析消息中的订单数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(),true);
                    //4.处理消息，创建订单
                    handleVoucherOrder(voucherOrder);
                    //5.ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1" , record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    //1.获取pending-list中的信息 xreadgroup group g1 c1 count 1 block 2000 stream stream.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2.判断消息获取是否成功,penging-list中没有消息，则结束循环
                    if (list == null || list.isEmpty()) {
                        break;
                    }
                    //3.解析消息中的订单数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //4.处理消息，创建订单
                    handleVoucherOrder(voucherOrder);
                    //5.ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //获取订单id
        Long orderId = redisIdWorker.nextId("order");
        //执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        //判断结果是否为0
        if (result != 0) {
            //不为0，如果为1，代表库存不足，如果为2，代表该用户已经下过单
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }
        //2.结果为0，有购买资格
        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //返回订单id
        return Result.ok(orderId);
    }
    /*    @Override
        public Result seckillVoucher(Long voucherId) {
            //获取用户
            Long userId = UserHolder.getUser().getId();
            //执行lua脚本
            Long result = stringRedisTemplate.execute(
                    SECKILL_SCRIPT,
                    Collections.emptyList(),
                    voucherId.toString(), userId.toString()
            );
            //判断结果是否为0
            if (result != 0) {
                //不为0，如果为1，代表库存不足，如果为2，代表该用户已经下过单
                return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
            }
            //2.结果为0，有购买资格，把下单信息保存到阻塞队列
            long orderId = redisIdWorker.nextId("order");
            //保存到阻塞队列
            VoucherOrder voucherOrder = new VoucherOrder();
            //2.1.订单id
            voucherOrder.setId(orderId);
            //2.2.用户id
            voucherOrder.setUserId(UserHolder.getUser().getId());
            //2.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            //2.4放入阻塞队列
            orderTasks.add(voucherOrder);
            //获取代理对象
            proxy = (IVoucherOrderService) AopContext.currentProxy();
            //返回订单id
            return Result.ok(orderId);
        }*/

    /*private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //5.1 查询订单
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        //SimpleRedisLock redisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if (!isLock) {
            //获取锁失败，返回失败或者重试
            log.error("不允许重复下单");
            return;
        }
        try {
                proxy.createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }
    }*/

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 添加参数校验
        if (voucherOrder == null) {
            log.error("订单信息为空，无法处理");
            return;
        }

        // 添加用户ID和优惠券ID校验
        if (voucherOrder.getUserId() == null || voucherOrder.getVoucherId() == null) {
            log.error("订单用户ID或优惠券ID为空，订单ID: {}", voucherOrder.getId());
            return;
        }

        // 获取锁 - 使用用户ID作为锁的key
        RLock lock = redissonClient.getLock("lock:order:" + voucherOrder.getUserId());

        // 获取锁
        boolean isLock = lock.tryLock();
        // 判断是否获取锁成功
        if (!isLock) {
            // 获取锁失败，返回失败或者重试
            log.error("用户 {} 不允许重复下单", voucherOrder.getUserId());
            return;
        }

        try {
            // 检查代理对象是否为null
            if (proxy == null) {
                log.error("代理对象未初始化");
                return;
            }

            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 释放锁
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }


    /*@Override
        public Result seckillVoucher(Long voucherId) {
            //1.查询优惠券
            SeckillVoucher voucher = seckillVoucherService.getById(voucherId);//数据库操作
            //2.判断秒杀是否开始
            if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
                return Result.fail("秒杀尚未开始");
            }
            //3.判断秒杀是否结束
            if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
                return Result.fail("秒杀已经结束");
            }
            //4.判断库存是否充足
            if (voucher.getStock() < 1) {
                return Result.fail("库存不足");
            }

            Long userId =UserHolder.getUser().getId();
            //创建锁对象
            //SimpleRedisLock redisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            //获取锁
            boolean isLock = lock.tryLock();
            //判断是否获取锁成功
            if (!isLock) {
                //获取锁失败，返回失败或者重试
                return Result.fail("不允许重复下单");
            }
            try {
                //获取代理对象（事务）
                IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
                return proxy.createVoucherOrder(voucherId);
            } finally {
                //释放锁
                lock.unlock();
            }
        }*/
    @Override
    @Transactional
    public void createVoucherOrder (VoucherOrder voucherOrder){
        //5.一人一单
        Long userId = voucherOrder.getUserId();

        //5.1 查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();//数据库操作
        //5.2 判断是否存在
        if (count > 0) {
            //用户已经买过了
            log.error("用户已经购买过一次");
        }
        //6.扣减库存  数据库操作
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1") //set stock=stock-1
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0) //where id = ? and stock>0
                .update();
        if (!success) {
            log.error("库存不足");
        }
        //7.创建订单
        save(voucherOrder);
    }
}

