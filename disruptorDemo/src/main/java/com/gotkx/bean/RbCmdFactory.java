package com.gotkx.bean;

import com.lmax.disruptor.EventFactory;
import lombok.RequiredArgsConstructor;

/**
 * @author HuangKai
 * @date 2021/7/18 16:04
 */

@RequiredArgsConstructor
public class RbCmdFactory implements EventFactory<RbCmd> {

    public RbCmd newInstance() {
        return RbCmd.builder()
                .code(0)
                .msg("")
                .build();
    }
}
