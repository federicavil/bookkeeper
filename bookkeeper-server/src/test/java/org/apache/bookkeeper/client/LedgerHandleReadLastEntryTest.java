package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class LedgerHandleReadLastEntryTest {

    private Object[][] entries;

    private static LedgerHandle ledgerHandle;
    private static Thread localBookkeeper;

    public LedgerHandleReadLastEntryTest(Object[][] entries){
        configure(entries);
    }

    private void configure(Object[][] entries) {
        this.entries = entries;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {new Object[][]{}},
                {new Object[][]{
                        {new byte[0],0,0}
                }},
                {new Object[][]{
                        {new byte[0],                              0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  0}
                }},
                {new Object[][]{
                        {new byte[0],                              0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  3}
                }},
                {new Object[][]{
                        {new byte[0],                              0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  3},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  4}

                }},
                {new Object[][]{
                        {new byte[0],                              0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  3},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  4},
                        {"test".getBytes(StandardCharsets.UTF_8),  3,  0}

                }},
                {new Object[][]{
                        {new byte[0],                              0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  3},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  4},
                        {"test".getBytes(StandardCharsets.UTF_8),  3,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  3,  1}

                }}
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
                    e.printStackTrace();
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
                //e.printStackTrace();
            }
        }
    }

    @Before
    public void ledgerConfiguration(){
        if(entries.length > 0){
            Object[] toAdd = entries[entries.length-1];
            try {
                ledgerHandle.addEntry((byte[]) toAdd[0], (Integer) toAdd[1],(Integer) toAdd[2]);
            } catch (InterruptedException | BKException e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
    }

    @Test
    public void readLastEntryTest(){
        try {
            LedgerEntry result = ledgerHandle.readLastEntry();
            Object[] lastEntryExpected = entries[entries.length-1];
            byte[] expectedData = new byte[(Integer)lastEntryExpected[2]];
            for (int i = 0; i < expectedData.length; i++){
                expectedData[i] = ((byte[])lastEntryExpected[0])[(int)lastEntryExpected[1]+i];
            }
            String expected = new String(expectedData);
            String actual = new String(result.getEntry());
            Assert.assertEquals(expected,actual);
            Assert.assertEquals((long)entries.length-1,result.getEntryId());
        } catch (InterruptedException | BKException e) {
            Assert.assertEquals(0,entries.length);
        }
    }

    @AfterClass
    public static void shutDown(){
        try {
            ledgerHandle.close();
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
        }
        localBookkeeper.interrupt();
    }
}
