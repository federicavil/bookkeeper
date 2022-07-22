package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RunWith(value = Parameterized.class)
public class LedgerHandleAddEntryTest {

    private byte[] data;
    private int offset;
    private int length;
    private boolean isExceptionExpected;
    // Iterazione Jacoco
    private boolean isLedgerHandleClosed;

    private static long entryId = -1L;

    private static LedgerHandle ledgerHandle;
    private static Thread localBookkeeper;
    private static Map<String, byte[]> customMetadata = new HashMap<>();

    public LedgerHandleAddEntryTest(byte[] data, int offset, int length, boolean isLedgerHandleClosed, boolean isExceptionExpected) {
        configure(data,offset,length,isLedgerHandleClosed,isExceptionExpected);
    }

    private void configure(byte[] data, int offset, int length,boolean isLedgerHandleClosed, boolean isExceptionExpected){
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.isExceptionExpected = isExceptionExpected;
        if(!isExceptionExpected)
            entryId++;

        //Iterazione Jacoco
        if(isLedgerHandleClosed){
            try {
                ledgerHandle.close();
            } catch (InterruptedException | BKException e) {
                e.printStackTrace();
            }
        }

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {      new byte[0],                             -1, -1, false, true},
                {      new byte[0],                             -1,  0, false, true},
                {      new byte[0],                             -1,  1, false, true},
                {      new byte[0],                              0,  0, false, false},
                {      new byte[0],                              0, -1, false, true},
                {      new byte[0],                              0,  1, false, true},
                {      new byte[0],                              1, -1, false, true},
                {      new byte[0],                              1,  0, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8), -1, -1, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8), -1,  0, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8), -1,  5, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  0, -1, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  0,  0, false, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  0,  3, false, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  0,  4, false, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  0,  5, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  3, -1, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  3,  0, false, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  3,  1, false, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  3,  2, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  4, -1, false, true},
                //{      "test".getBytes(StandardCharsets.UTF_8),  4,  0, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  4,  1, false, true},
                //Iterazione Jacoco
                {      "test".getBytes(StandardCharsets.UTF_8),  3,  1, true, true},


        });
    }

    @BeforeClass
    public static void createLedgerHandle(){
        boolean connected = false;
        localBookkeeper = new Thread(new Runnable() {
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
        while(!connected){
            try{
                Thread.sleep(1000);
                BookKeeper bk = new BookKeeper("127.0.0.1:2181");
                ledgerHandle = bk.createLedger(1,1,1,BookKeeper.DigestType.MAC,"test".getBytes(StandardCharsets.UTF_8));
                connected = true;

            } catch (Exception e) {
                System.out.println("ledger fail");
            }
        }
    }

    @Test
    public void addEntryTest(){
        Exception exception = null;
        long result = -1L;
        try{
            result = ledgerHandle.addEntry(this.data, this.offset, this.length);
        }catch(InterruptedException | BKException e){
            exception = e;
        }catch(Exception e){
            exception = e;
        }
        if(isExceptionExpected)
            Assert.assertNotNull(exception);
        else {
            Assert.assertNull(exception);
            Assert.assertEquals(entryId,result);
        }
    }

    /*
    @AfterClass
    public static void shutDown(){
        try {
            ledgerHandle.close();
        } catch (InterruptedException | BKException e) {
            System.out.println("fail");
        }
        localBookkeeper.interrupt();
    }
     */
}
