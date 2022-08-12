package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;

import java.nio.charset.StandardCharsets;

public class BookkeeperInstance {

    private static BookKeeper bk = null;

    private BookkeeperInstance(){
        Thread localBookkeeper = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ServerConfiguration conf = new ServerConfiguration();
                    conf.setAllowLoopback(true);
                    LocalBookKeeper.startLocalBookies("127.0.0.1",2181,1,true,5000,conf);
                } catch (Exception e) {
                    System.out.println("bookkeeper fail");
                }
            }
        });
        localBookkeeper.start();
        try{
            Thread.sleep(40000);
            bk = new BookKeeper("127.0.0.1:2181");
            Thread.sleep(40000);

        } catch (Exception ignored) {

        }

    }

    public static synchronized BookKeeper getBookkeeperInstance(){
        if(bk == null){
            new BookkeeperInstance();
        }
        return bk;
    }
}
