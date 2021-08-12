package com.gotkx.engine.bean;

import com.alipay.remoting.exception.CodecException;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.util.Bits;
import com.google.common.collect.Lists;
import com.gotkx.engine.core.EngineApi;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import thirdpart.bean.CmdPack;
import thirdpart.codec.IBodyCodec;
import thirdpart.order.OrderCmd;

import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * @author HuangKai
 * @date 2021/8/7 18:43
 */
@Log4j2
public class CmdPacketQueue {

    private static CmdPacketQueue ourInstance = new CmdPacketQueue();

    private CmdPacketQueue(){};

    public static CmdPacketQueue getInstance(){
        return ourInstance;
    }

    //////////////////////////////////////////////////////////////////////////////
    private final BlockingDeque<CmdPack> recvCache = new LinkedBlockingDeque<>();

    private RheaKVStore orderKVStore;
    private IBodyCodec codec;
    private EngineApi engineApi;

    private long lastPackNo = -1;

    /**
     * 数据填充
     * @param cmdPack
     */
    public void cache(CmdPack cmdPack){
        recvCache.offer(cmdPack);
    }

    //////////////////////////////////////////////////////////////////////////////
    public void init(RheaKVStore orderKVStore, IBodyCodec codec, EngineApi engineApi){
        this.codec = codec;
        this.orderKVStore = orderKVStore;
        this.engineApi = engineApi;

        new Thread(() -> {
            while (true){
                try {
                    CmdPack cmdPack = recvCache.poll(10, TimeUnit.SECONDS);
                    if(cmdPack != null){
                        handle(cmdPack);
                    }
                } catch (Exception e){
                    log.error("msg packet recvCache error, continue",e);
                }
            }
        }).start();
    }

    private void handle(CmdPack cmdPack) throws CodecException {
        log.info("recv : {}", cmdPack);

        // NACK
        long packNo = cmdPack.getPackNo();
        if(packNo == lastPackNo + 1){
            if(CollectionUtils.isEmpty(cmdPack.getOrderCmds())){
                return;
            }
            for(OrderCmd orderCmd : cmdPack.getOrderCmds()){
                engineApi.submitCommand(orderCmd);
            }
        }else if(packNo <= lastPackNo){
            // 来自历史的重复的包
            log.warn("recv duplicate packId : {}",packNo);
        }else {
            // 跳号
            log.info("packNo lost from {} to {}, begin query from sequencer", lastPackNo + 1,packNo);
            // 请求缺少的数据
            byte[] firstKey = new byte[8];
            Bits.putLong(firstKey,0,lastPackNo + 1);

            byte[] lastKey = new byte[8];
            Bits.putLong(lastKey,0,packNo + 1);

            List<KVEntry> kvEntries = orderKVStore.bScan(firstKey, lastKey);
            if(CollectionUtils.isNotEmpty(kvEntries)){
                List<CmdPack> collect = Lists.newArrayList();
                for (KVEntry entry : kvEntries) {
                    byte[] value = entry.getValue();
                    if(ArrayUtils.isNotEmpty(value)){
                        collect.add(codec.deserialize(value, CmdPack.class));
                    }
                }
                collect.sort((o1, o2) -> (int)(o1.getPackNo() - o2.getPackNo()));
                for (CmdPack pack : collect) {
                    if(CollectionUtils.isEmpty(pack.getOrderCmds())){
                        continue;
                    }
                    for(OrderCmd orderCmd : pack.getOrderCmds()){
                        engineApi.submitCommand(orderCmd);
                    }
                }
            }else {
                // 排队机出错 导致出现了跳号
                lastPackNo = packNo;
            }
        }
    }

}
