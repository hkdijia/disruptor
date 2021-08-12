package com.gotkx.engine.bean.orderbook;

import com.gotkx.engine.bean.command.RbCmd;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * @author HuangKai
 * @date 2021/8/8 13:13
 */
public interface IOrderBucket extends Comparable<IOrderBucket>{

    AtomicLong tidGen = new AtomicLong(0);

    /**
     * 1.新增订单
     * @param order
     */
    void put(Order order);

    /**
     * 移除订单
     * @param oid
     * @return
     */
    Order remove(long oid);

    /**
     *  撮合匹配
     * @param volumeLeft   匹配的数量
     * @param triggerCmd   传递来的委托
     * @param removeOrderCallback   回调函数
     * @return
     */
    long match(long volumeLeft, RbCmd triggerCmd, Consumer<Order> removeOrderCallback);

    /**
     * 获取价格
     * @return
     */
    long getPrice();

    /**
     * 设置价格
     * @param price
     */
    void setPrice(long price);

    /**
     * 总量
     * @return
     */
    long getTotalVolume();

    /**
     * 5.初始化选项
     * @param type
     * @return
     */
    static IOrderBucket create(OrderBucketImplType type) {
        switch (type) {
            case GUDY:
                return new GOrderBucketImpl();
            default:
                throw new IllegalArgumentException();
        }
    }

    @Getter
    enum OrderBucketImplType {
        GUDY(0);

        private byte code;

        OrderBucketImplType(int code) {
            this.code = (byte) code;
        }
    }

    /**
     * 比较 排序
     * @param other
     * @return
     */
    @Override
    default int compareTo(IOrderBucket other) {
        return Long.compare(this.getPrice(), other.getPrice());
    }

}
