package org.apache.bookkeeper.client;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;

public class LedgerHandleTest extends BookKeeperClusterTestCase {

    private static final int BOOKIES = 1;
    protected LedgerHandle ledgerHandle;

    public LedgerHandleTest(){
        super(BOOKIES);
    }

    @Before
    public void createLedger(){
        try {
            ledgerHandle = bkc.createLedger(1,1,1,BookKeeper.DigestType.MAC,"test".getBytes(StandardCharsets.UTF_8));
        } catch (BKException | InterruptedException | RuntimeException e) {
            e.printStackTrace();
        }
    }

    protected void closeLedgerHandle(){
        //Iterazione Jacoco
        try {
            ledgerHandle.close();
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
        }

    }

    @After
    public void shutDown(){
        try {
            bkc.close();
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
        }
    }


}
