package com.gotkx.engine.bean;

import com.alipay.remoting.exception.CodecException;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.configured.MultiRegionRouteTableOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured;
import com.gotkx.engine.core.EngineApi;
import com.gotkx.engine.db.DbQuery;
import com.gotkx.engine.handler.risk.ExistRiskHandler;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbutils.QueryRunner;
import thirdpart.bean.CmdPack;
import thirdpart.checksum.IChecksum;
import thirdpart.codec.IBodyCodec;
import thirdpart.codec.IMsgCodec;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;


/**
 * @author HuangKai
 * @date 2021/8/7 17:44
 */

@Log4j2
@ToString
@Getter
@RequiredArgsConstructor
public class EngineConfig {

    private short id;
    private String orderRecvIp;
    private int orderRecvPort;
    private String seqUrIList;
    private String pubIp;
    private int pubPort;

    @NonNull
    private String fileName;

    @NonNull
    private IBodyCodec bodyCodec;

    @NonNull
    private IChecksum cs;

    @NonNull
    private IMsgCodec msgCodec;

    @Getter
    private DbQuery db;

    private Vertx vertx = Vertx.vertx();

    @Getter
    @ToString.Exclude
    private final RheaKVStore orderKvStore = new DefaultRheaKVStore();

    @Getter
    private EngineApi engineApi = new EngineApi();

    public void startup() throws Exception {
        //  1. ??????????????????
        initConfig();

        //  2.???????????????
        initDB();

        //  3.??????????????????
        startEngine();

        //  4.?????????????????? ????????????????????????

        //  5.????????????????????????????????????
        startSeqConn();
    }

    private void startEngine() throws Exception {
        //  1.?????????????????????
        ExistRiskHandler riskHandler = new ExistRiskHandler(
                db.queryAllBalance().keySet(),
                db.queryAllStockCode()
        );

        //  2.???????????????

        //  3.???????????????
    }


    /**
     * ???????????????
     */
    private void initDB() {
        QueryRunner queryRunner = new QueryRunner(new ComboPooledDataSource());
        db = new DbQuery(queryRunner);
    }

    /**
     * ???????????? udp
     * @return
     */
    private static NetworkInterface mainInterface() throws SocketException {
        final ArrayList<NetworkInterface> interfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
        final NetworkInterface networkInterface = interfaces.stream().filter(t -> {
            // 1. ???loopback??????
            // 2. ??????multicast ??????
            // 3. ??????????????????
            // 4. ???ipv4 ??????
            try {
                final boolean isLoopback = t.isLoopback();
                final boolean supportMulticast = t.supportsMulticast();
                final boolean isVirtualBox = t.getDisplayName().contains("VirtualBox") || t.getDisplayName().contains("Host-only");
                final boolean hasIpv4 = t.getInterfaceAddresses().stream().anyMatch(ia -> ia.getAddress() instanceof Inet4Address);
                return !isLoopback && supportMulticast && !isVirtualBox && hasIpv4;
            } catch (Exception e){
                log.error("fine net interface error",e);
            }
            return false;
        }).sorted(Comparator.comparing(NetworkInterface::getName)).findFirst().orElse(null);
        return networkInterface;
    }

    private void startSeqConn(){
        final List<RegionRouteTableOptions> regionRouteTableOptions =  MultiRegionRouteTableOptionsConfigured
                                                                        .newConfigured()
                                                                        .withInitialServerList(-1L,seqUrIList)
                                                                        .config();

        final PlacementDriverOptions placementDriverOptions = PlacementDriverOptionsConfigured
                                                                        .newConfigured()
                                                                        .withFake(true)
                                                                        .withRegionRouteTableOptionsList(regionRouteTableOptions)
                                                                        .config();

        final RheaKVStoreOptions rheaKVStoreOptions = RheaKVStoreOptionsConfigured
                                                                        .newConfigured()
                                                                        .withPlacementDriverOptions(placementDriverOptions)
                                                                        .config();
        orderKvStore.init(rheaKVStoreOptions);

        //  ?????????????????????
        CmdPacketQueue.getInstance().init(orderKvStore,bodyCodec,engineApi);

        //  ?????? ????????????Socket?????????????????????
        DatagramSocket datagramSocket = vertx.createDatagramSocket(new DatagramSocketOptions());
        datagramSocket.listen(orderRecvPort, "0.0.0.0", asyncRes -> {
            if(asyncRes.succeeded()){

                datagramSocket.handler(packet -> {
                    Buffer data = packet.data();
                    if(data.length() > 0){
                        try {
                            CmdPack cmdPack = bodyCodec.deserialize(data.getBytes(), CmdPack.class);
                            CmdPacketQueue.getInstance().cache(cmdPack);
                        } catch (CodecException e) {
                            log.error("decode packet error",e);
                        }
                    }else {
                        log.error("recv empty upd packet from client : {}", packet.sender().toString());
                    }
                });

                try {
                    datagramSocket.listenMulticastGroup(
                            orderRecvIp,
                            mainInterface().getName(),
                            null,
                            asyncRes2 -> {
                               log.info("listen succeed {}", asyncRes2.succeeded());
                            });
                } catch (Exception e){
                    log.error(e);
                }
            }else {
                log.error("Listen failed,", asyncRes.cause());
            }
        });
    }



    /**
     * ?????????
     * @throws IOException
     */
    private void initConfig() throws IOException {
        Properties properties = new Properties();
        properties.load(this.getClass().getResourceAsStream("/"+fileName));

        id = Short.parseShort(properties.getProperty("id"));
        orderRecvIp = properties.getProperty("orderRecvIp");
        orderRecvPort = Integer.parseInt(properties.getProperty("orderRecvPort"));
        seqUrIList = properties.getProperty("seqUrlList");
        pubIp = properties.getProperty("pubIp");
        pubPort = Integer.parseInt(properties.getProperty("pubPort"));
        log.info(this);
    }

}
