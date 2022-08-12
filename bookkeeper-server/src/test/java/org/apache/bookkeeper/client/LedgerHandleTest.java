package org.apache.bookkeeper.client;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;

public class LedgerHandleTest extends BookKeeperClusterTestCase {

    private static final int BOOKIES = 1;
    protected LedgerHandle ledgerHandle;
    protected boolean isLedgerHandleClosed;

    public LedgerHandleTest(boolean isLedgerHandleClosed){
        super(BOOKIES);
        this.isLedgerHandleClosed = isLedgerHandleClosed;
    }

    @Before
    public void createLedger(){
        try {
            ledgerHandle = bkc.createLedger(1,1,1,BookKeeper.DigestType.MAC,"test".getBytes(StandardCharsets.UTF_8));
        } catch (BKException | InterruptedException | RuntimeException e) {
            e.printStackTrace();
        }
        //Iterazione Jacoco
        if(isLedgerHandleClosed){
            try {
                ledgerHandle.close();
            } catch (InterruptedException | BKException e) {
                e.printStackTrace();
            }
        }
    }

//    @After
//    public void shutDown(){
//        try {
//            if(!isLedgerHandleClosed)
//                ledgerHandle.close();
//            bkc.close();
//        } catch (InterruptedException | BKException e) {
//            e.printStackTrace();
//        }
//    }


}
