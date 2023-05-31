package org.hiwei;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;

public class QuorumPeerMain {
    private static Logger log = LoggerFactory.getLogger(QuorumPeerMain.class);
    protected static String configFileStr = null;
    public static void main( String[] args ) {
        log.info("QuorumPeerMain start...");
        Properties cfg = parseConfig(args[0]);
        QuorumPeer quorumPeer = new QuorumPeer(cfg);
        //解析配置文件建立连接
        quorumPeer.listener.start();
        //开启选举线程
        Executors.newSingleThreadExecutor().execute(() -> {
            quorumPeer.startVoteSendListener();
            quorumPeer.election();
        });
        log.info("QuorumPeerMain start success!");
    }

    /**
     * 配置文件解析
     * @param arg
     * @return
     */
    private static Properties parseConfig(String arg) {
        String path = arg;
        File configFile = new File(path);
        Properties cfg = new Properties();
        try {
            try (FileInputStream in = new FileInputStream(configFile)) {
                cfg.load(in);
                configFileStr = path;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cfg;
    }

}
