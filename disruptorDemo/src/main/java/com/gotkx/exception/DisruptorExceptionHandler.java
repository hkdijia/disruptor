package com.gotkx.exception;

import com.lmax.disruptor.ExceptionHandler;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.util.BiConsumer;

/**
 * @author HuangKai
 * @date 2021/7/18 17:19
 */

@Log4j2
@AllArgsConstructor
public class DisruptorExceptionHandler<T> implements ExceptionHandler<T> {

    public final String name;
    public final BiConsumer<Throwable,Long> onException;

    @Override
    public void handleEventException(Throwable throwable, long sequence, T t) {
        if(log.isDebugEnabled()){
            log.debug("Disruptor '{}' seq = {} exist exception", name, sequence);
        }
        onException.accept(throwable,sequence);
    }

    @Override
    public void handleOnStartException(Throwable throwable) {
        log.info("Disruptor '{}' start exist exception", name);
    }

    @Override
    public void handleOnShutdownException(Throwable throwable) {
        log.info("Disruptor '{}' shutdown exist exception", name);
    }
}
