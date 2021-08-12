package com.gotkx.engine;

import com.gotkx.engine.bean.EngineConfig;
import thirdpart.checksum.ByteCheckSum;
import thirdpart.codec.BodyCodec;
import thirdpart.codec.MsgCodec;

/**
 * @author HuangKai
 * @date 2021/8/7 23:32
 */
public class EngineStartup {

    public static void main(String[] args) throws Exception {
        new EngineConfig(
                "engine.properties",
                new BodyCodec(),
                new ByteCheckSum(),
                new MsgCodec()
        ).startup();
    }

}
