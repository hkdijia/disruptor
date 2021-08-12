package com.gotkx.engine.handler.risk;

import com.gotkx.engine.bean.command.CmdResultCode;
import com.gotkx.engine.bean.command.RbCmd;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import thirdpart.order.CmdType;

/**
 * 前置风控处理
 * @author HuangKai
 * @date 2021/8/8 12:06
 */

@Log4j2
@RequiredArgsConstructor
public class ExistRiskHandler extends BaseHandler{

    /**
     * 用户id集合
     */
    @NonNull
    private MutableLongSet uidSet;

    /**
     * 股票代码集合
     */
    @NonNull
    private MutableIntSet codeSet;

    @Override
    public void onEvent(RbCmd rbCmd, long sequence, boolean endOfBatch) throws Exception {
        if(rbCmd.command == CmdType.HQ_PUB){
            return;
        }

        //  1。用户是否存在
        if(rbCmd.command == CmdType.NEW_ORDER || rbCmd.command == CmdType.CANCEL_ORDER){
            if(!uidSet.contains(rbCmd.uid)){
                log.error("illegal uid[{}] exist",rbCmd.uid);
                rbCmd.resultCode = CmdResultCode.RISK_INVALID_USER;
                return;
            }
        }

        //  2。股票是否合法
        if(!codeSet.contains(rbCmd.code)){
            log.error("illegal code[{}] exist",rbCmd.code);
            rbCmd.resultCode = CmdResultCode.RISK_INVALID_CODE;
            return;
        }

    }
}
