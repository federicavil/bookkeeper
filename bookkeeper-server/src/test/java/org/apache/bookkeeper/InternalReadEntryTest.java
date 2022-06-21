package org.apache.bookkeeper;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.utils.TestBKConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;


@RunWith(value= Parameterized.class)
public class InternalReadEntryTest {

    private long ledgerId;
    private long entryId;
    private long location;
    private boolean validateEntry;
    private boolean isExceptionExpected;
    private int expectedValue;

    private EntryLogger entryLogger;
    private MockBookKeeper bkMock;
    private File rootDir;
    private File curDir;

    public InternalReadEntryTest(IdType ledgerId, IdType entryId, long location, boolean validateEntry, boolean isExceptionExpected) throws Exception {
        configure(ledgerId, entryId, location, validateEntry, isExceptionExpected);
    }

    private void configure(IdType ledgerId, IdType entryId, long location, boolean validateEntry, boolean isExceptionExpected){
        switch(entryId){
            case INEXISTENT:
                this.entryId = 0L;
                break;
            case INVALID:
                this.entryId = -1L;
                break;
            case EXISTING:
                createNewEntry();
                break;
        }


        switch(ledgerId){
            case INEXISTENT:
                this.ledgerId = 0L;
                break;
            case INVALID:
                this.ledgerId = -1L;
                break;
            case EXISTING:
                // Già settato dal metodo di creazione della entry
                break;
        }

        this.validateEntry = validateEntry;
        this.location = location;
        this.isExceptionExpected = isExceptionExpected;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
                //LedgerId, EntryId, Location, ValidateEntry, isExceptionExpected
                {IdType.INVALID,IdType.INVALID,-1L,false,true},
                {IdType.INVALID,IdType.INVALID,0L,false,true},
                {IdType.INVALID,IdType.INVALID,1L,false,true},
                {IdType.INVALID,IdType.EXISTING,-1L,false,true},
                {IdType.INVALID,IdType.EXISTING,0L,false,true},
                {IdType.INVALID,IdType.EXISTING,1L,false,true},
                {IdType.INVALID,IdType.INEXISTENT,-1L,false,true},
                {IdType.INVALID,IdType.INEXISTENT,0L,false,true},
                {IdType.INVALID,IdType.INEXISTENT,1L,false,true},
                {IdType.EXISTING,IdType.INVALID,-1L,true,true},
                {IdType.EXISTING,IdType.INVALID,0L,true,true},
                {IdType.EXISTING,IdType.INVALID,1L,true,true},
                {IdType.EXISTING,IdType.EXISTING,-1L,true,true},
                //{IdType.EXISTING,IdType.EXISTING,0L,true,false},
                //{IdType.EXISTING,IdType.EXISTING,1L,true,false},
                {IdType.EXISTING,IdType.INEXISTENT,-1L,true,true},
                {IdType.EXISTING,IdType.INEXISTENT,0L,true,true},
                {IdType.EXISTING,IdType.INEXISTENT,1L,true,true},
                {IdType.INEXISTENT,IdType.INVALID,-1L,false,true},
                {IdType.INEXISTENT,IdType.INVALID,0L,false,true},
                {IdType.INEXISTENT,IdType.INVALID,1L,false,true},
                {IdType.INEXISTENT,IdType.EXISTING,-1L,false,true},
                {IdType.INEXISTENT,IdType.EXISTING,0L,false,true},
                {IdType.INEXISTENT,IdType.EXISTING,1L,false,true},
                {IdType.INEXISTENT,IdType.INEXISTENT,-1L,false,true},
                {IdType.INEXISTENT,IdType.INEXISTENT,0L,false,true},
                {IdType.INEXISTENT,IdType.INEXISTENT,1L,false,true},
        });
    }

    @Before
    public void setUpBookkeeper(){
        try {

            this.rootDir = IOUtils.createTempDir("internalReadEntryTest",".dir");
            this.curDir = Bookie.getCurrentDirectory(rootDir);
            Bookie.checkDirectoryStructure(curDir);

            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            this.entryLogger = new EntryLogger(conf, new LedgerDirsManager(conf, new File[]{rootDir},new DiskChecker(
                    conf.getDiskUsageThreshold(),
                    conf.getDiskUsageWarnThreshold())));
        } catch (Exception e) {
            e.printStackTrace();
            //Assert.fail();
        }

    }

    private void createNewEntry(){
        try {
            this.bkMock = new MockBookKeeper(null);
            LedgerHandle ledgerHandle = this.bkMock.createLedger(BookKeeper.DigestType.MAC, "test".getBytes(StandardCharsets.UTF_8));
            this.ledgerId = ledgerHandle.getId();
            ByteBuffer entry = ByteBuffer.allocate(4);
            this.expectedValue = 1;
            entry.putInt(this.expectedValue);
            entry.position(0);
            this.entryId = ledgerHandle.addEntry(entry.array());
            ledgerHandle.close();
        }catch (Exception e) {
            e.printStackTrace();
            //Assert.fail();
        }
    }

    @Test
    public void test_internalReadEntry(){
        Exception exception = null;
        ByteBuf result = null;
        try{
            result = this.entryLogger.internalReadEntry(this.ledgerId,this.entryId,this.location,this.validateEntry);
        }catch(IOException e){
            exception = e;
        }
        if(isExceptionExpected)
            Assert.assertNotNull(exception);
        else{
            Assert.assertEquals(result.getInt(0),this.expectedValue);
        }
    }

    @After
    public void tearDown(){
        try {
            this.entryLogger.shutdown();
            FileUtils.deleteDirectory(this.curDir);
            FileUtils.deleteDirectory(this.rootDir);
            //bkMock.close();
        } catch ( IOException e) {
            e.printStackTrace();
            //Assert.fail();
        }
    }

    private enum IdType{
        INVALID,
        INEXISTENT,
        EXISTING
    }

}

