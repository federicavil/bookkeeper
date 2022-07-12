package org.apache.bookkeeper.client;

import net.bytebuddy.asm.Advice;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.checkerframework.checker.units.qual.C;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

@RunWith(value = Parameterized.class)
public class AddEntryTest {

    private long entryId;
    private byte[] data;
    private int offset;
    private int length;
    private boolean isExceptionExpected;
    private long expectedValue;

    private static LedgerHandle ledgerHandle;

    private static BookKeeper bk;

    public AddEntryTest(EntryIdType entryId, byte[] data, int offset, int length, boolean isExceptionExpected) {
        configure(entryId,data,offset,length,isExceptionExpected);
    }

    private void configure(EntryIdType entryId, byte[] data, int offset, int length, boolean isExceptionExpected){
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.isExceptionExpected = isExceptionExpected;
        //this.expectedValue = expectedValue;

        switch(entryId){
            case INVALID:
                this.entryId = -1L;
                break;
            case NOT_EXISTENT:
                this.entryId = 0L;
                break;
            case ALREADY_EXISTENT:
                //aggiungo prima una nuova entry con lo stesso id di quella che aggiungerò dopo
                this.entryId = 0L;
                break;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {EntryIdType.NOT_EXISTENT,      new byte[0],                             -1, -1, true},
                {EntryIdType.NOT_EXISTENT,      new byte[0],                             -1,  0, true},
                {EntryIdType.NOT_EXISTENT,      new byte[0],                             -1,  1, true},
                {EntryIdType.NOT_EXISTENT,      new byte[0],                              0, -1, true},
                //{EntryIdType.NOT_EXISTENT,      new byte[0],                              0,  0, false},
                {EntryIdType.NOT_EXISTENT,      new byte[0],                              0,  1, true},
                {EntryIdType.NOT_EXISTENT,      "test".getBytes(StandardCharsets.UTF_8), -1, -1, true},
                {EntryIdType.NOT_EXISTENT,      "test".getBytes(StandardCharsets.UTF_8), -1,  0, true},
                {EntryIdType.NOT_EXISTENT,      "test".getBytes(StandardCharsets.UTF_8), -1,  5, true},
                {EntryIdType.NOT_EXISTENT,      "test".getBytes(StandardCharsets.UTF_8),  0, -1, true},
                //{EntryIdType.NOT_EXISTENT,      "test".getBytes(StandardCharsets.UTF_8),  0,  0, false},
                {EntryIdType.NOT_EXISTENT,      "test".getBytes(StandardCharsets.UTF_8),  0,  4, false},
                {EntryIdType.NOT_EXISTENT,      "test".getBytes(StandardCharsets.UTF_8),  3, -1, true},
                //{EntryIdType.NOT_EXISTENT,      "test".getBytes(StandardCharsets.UTF_8),  3,  0, false},
                {EntryIdType.NOT_EXISTENT,      "test".getBytes(StandardCharsets.UTF_8),  3,  1, false},
                {EntryIdType.NOT_EXISTENT,      "test".getBytes(StandardCharsets.UTF_8),  3,  2, true},

        });
    }

    @BeforeClass
    public static void createLedgerHandle() {
        boolean connected = false;
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        ServerConfiguration conf = new ServerConfiguration();
                        conf.setAllowLoopback(true);
                        LocalBookKeeper.startLocalBookies("127.0.0.1",2181,1,true,5000,conf);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            thread.start();
        while(!connected){
            try{
                Thread.sleep(1000);
                bk = new BookKeeper("127.0.0.1:2181");
                ledgerHandle = bk.createLedger(1,1,1,BookKeeper.DigestType.MAC,"test".getBytes(StandardCharsets.UTF_8));
                connected = true;

            } catch (Exception e) {
                //e.printStackTrace();
            }
        }


    }

    @Test
    public void addEntryTest(){
        Exception exception = null;
        long result;
        try{
            result = ledgerHandle.addEntry(this.data, this.offset, this.length);
            //Assert.assertEquals(this.expectedValue,result);
        }catch(InterruptedException | BKException e){
            exception = e;
        }catch(Exception e){
            exception = e;
        }
        if(isExceptionExpected)
            Assert.assertNotNull(exception);
        else Assert.assertNull(exception);
    }

    private enum EntryIdType{
        INVALID,
        ALREADY_EXISTENT,
        NOT_EXISTENT
    }
}
