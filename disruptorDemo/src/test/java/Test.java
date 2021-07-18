import com.gotkx.bean.RbCmd;
import com.gotkx.bean.RbCmdFactory;
import com.gotkx.bean.RbData;
import com.gotkx.exception.DisruptorExceptionHandler;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.extern.log4j.Log4j2;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * @author HuangKai
 * @date 2021/7/18 16:12
 */

@Log4j2
public class Test {
    public static void main(String[] args) {
        new Test().initDisruptor();
    }

    private Disruptor disruptor;

    private void initDisruptor(){
        disruptor = new Disruptor(
          // event factory
          new RbCmdFactory(),
          1024,
          // 线程池
          new AffinityThreadFactory("aft_core", AffinityStrategies.ANY),
          //  1个生产者线程
          ProducerType.SINGLE,
          new BlockingWaitStrategy()
        );

        // 全局异常处理器
        DisruptorExceptionHandler<RbCmd> exceptionHandler = new DisruptorExceptionHandler<>("disruptor-1",
                (exception, seq) -> {
                    log.error("Exception thrown on seq={}", seq, exception);
                });
        disruptor.setDefaultExceptionHandler(exceptionHandler);

        // 定义消费者 消费排序
        ConsumerA consumerA = new ConsumerA();
        ConsumerB consumerB = new ConsumerB();
        disruptor.handleEventsWith(consumerA).then(consumerB);

        disruptor.start();

        // 定义生产者  1s发布一条数据
        new Timer().schedule(new ProducerTask(),2000,1000);
    }

    private int index = 0;

    /**
     * 生产者线程
     */
    private class ProducerTask extends TimerTask{
        @Override
        public void run() {
            disruptor.getRingBuffer().publishEvent(PUB_TRANSLATOR, new RbData(index, "hello world"));
            index++;
        }
    }

    private static final EventTranslatorOneArg<RbCmd, RbData> PUB_TRANSLATOR = ((rbCmd, seq, rbData) -> {
        rbCmd.code = rbData.code;
        rbCmd.msg = rbData.msg;
    });

    /**
     * 消费线程A
     */
    private class ConsumerA implements EventHandler<RbCmd>{
        @Override
        public void onEvent(RbCmd rbCmd, long seq, boolean b) throws Exception {
            log.info("ConsumerA recv : {}", rbCmd);
        }
    }

    /**
     * 消费线程B
     */
    private class ConsumerB implements EventHandler<RbCmd>{
        @Override
        public void onEvent(RbCmd rbCmd, long seq, boolean b) throws Exception {
            log.info("ConsumerB recv : {}", rbCmd);
        }
    }

}
