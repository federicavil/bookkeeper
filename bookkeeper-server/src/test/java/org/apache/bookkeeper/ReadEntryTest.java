package org.apache.bookkeeper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.utils.TestBKConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;


@RunWith(value= Parameterized.class)
public class ReadEntryTest {

    private final int HEADER_SIZE = 2*(Long.BYTES) + (Integer.BYTES); //ledgerIdSize, EntryIdSize, Length

    private long ledgerId;
    private long entryId;
    private long location;
    private boolean isExceptionExpected;
    private byte[] expectedValue;

    private long ledgerIdMatching;
    private long entryIdMatching;
    private long locationMatching;
    private static EntryLogger entryLogger;
    private static MockBookKeeper bkMock;
    private static List<File> dirs;
    //Seconda iterazione Jacoco
    private static int maxSizeLogger;

    public ReadEntryTest(IdType ledgerId, IdType entryId, IdType location, boolean isExceptionExpected) throws Exception {
        configure(ledgerId, entryId, location, isExceptionExpected);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
                //LedgerId              EntryId,                Location,                isExceptionExpected
                {IdType.INVALID,        IdType.INVALID,         IdType.INVALID,          true},
                {IdType.INVALID,        IdType.INVALID,         IdType.NOT_MATCHING,     true},
                {IdType.INVALID,        IdType.INVALID,         IdType.MATCHING,         true},
                {IdType.INVALID,        IdType.NOT_MATCHING,    IdType.INVALID,          true},
                {IdType.INVALID,        IdType.NOT_MATCHING,    IdType.NOT_MATCHING,     true},
                {IdType.INVALID,        IdType.NOT_MATCHING,    IdType.MATCHING,         true},
                {IdType.INVALID,        IdType.MATCHING,        IdType.INVALID,          true},
                {IdType.INVALID,        IdType.MATCHING,        IdType.NOT_MATCHING,     true},
                {IdType.INVALID,        IdType.MATCHING,        IdType.MATCHING,         true},
                {IdType.NOT_MATCHING,   IdType.INVALID,         IdType.INVALID,          true},
                {IdType.NOT_MATCHING,   IdType.INVALID,         IdType.NOT_MATCHING,     true},
                {IdType.NOT_MATCHING,   IdType.INVALID,         IdType.MATCHING,         true},
                {IdType.NOT_MATCHING,   IdType.NOT_MATCHING,    IdType.INVALID,          true},
                {IdType.NOT_MATCHING,   IdType.NOT_MATCHING,    IdType.NOT_MATCHING,     true},
                {IdType.NOT_MATCHING,   IdType.NOT_MATCHING,    IdType.MATCHING,         true},
                {IdType.NOT_MATCHING,   IdType.MATCHING,        IdType.INVALID,          true},
                {IdType.NOT_MATCHING,   IdType.MATCHING,        IdType.NOT_MATCHING,     true},
                {IdType.NOT_MATCHING,   IdType.MATCHING,        IdType.MATCHING,         true},
                {IdType.MATCHING,       IdType.INVALID,         IdType.INVALID,          true},
                {IdType.MATCHING,       IdType.INVALID,         IdType.NOT_MATCHING,     true},
                {IdType.MATCHING,       IdType.INVALID,         IdType.MATCHING,         true},
                {IdType.MATCHING,       IdType.NOT_MATCHING,    IdType.INVALID,          true},
                {IdType.MATCHING,       IdType.NOT_MATCHING,    IdType.NOT_MATCHING,     true},
                {IdType.MATCHING,       IdType.NOT_MATCHING,    IdType.MATCHING,         true},
                {IdType.MATCHING,       IdType.MATCHING,        IdType.INVALID,          true},
                {IdType.MATCHING,       IdType.MATCHING,        IdType.NOT_MATCHING,     true},
                {IdType.MATCHING,       IdType.MATCHING,        IdType.MATCHING,         false},
                //Seconda iterazione jacoco
                {IdType.MATCHING,       IdType.MATCHING,        IdType.OVERSIZE,         true},
        });
    }

    private void configure(IdType ledgerId, IdType entryId, IdType location, boolean isExceptionExpected){
        createNewEntry();
        switch(ledgerId){
            case INVALID:
                this.ledgerId = -1L;
                break;
            case MATCHING:
                this.ledgerId = this.ledgerIdMatching;
                break;
            case NOT_MATCHING:
                this.ledgerId = createNewLedger().getId();
                break;
        }

        switch(entryId){
            case INVALID:
                this.entryId = -1L;
                break;
            case MATCHING:
                this.entryId = this.entryIdMatching;
                break;
            case NOT_MATCHING:
                this.entryId = 1L;
                break;
        }

        switch(location){
            case INVALID:
                this.location = 1023L;
                break;
            case MATCHING:
                this.location = this.locationMatching;
                break;
            case NOT_MATCHING:
                this.location = this.locationMatching+1;
                break;
            //Seconda iterazione jacoco
            case OVERSIZE:
                this.location = maxSizeLogger;
        }

        this.isExceptionExpected = isExceptionExpected;
    }

    @BeforeClass
    public static void setUpBookkeeper(){
        try {
            File rootDir = IOUtils.createTempDir("readEntryTest",".dir");
            File curDir = Bookie.getCurrentDirectory(rootDir);
            Bookie.checkDirectoryStructure(curDir);
            dirs = new ArrayList<>();
            dirs.add(rootDir);
            dirs.add(curDir);

            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            maxSizeLogger = conf.getNettyMaxFrameSizeBytes();
            entryLogger = new EntryLogger(conf, new LedgerDirsManager(conf, new File[]{rootDir},new DiskChecker(
                    conf.getDiskUsageThreshold(),
                    conf.getDiskUsageWarnThreshold())));

            bkMock = new MockBookKeeper(null);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private LedgerHandle createNewLedger(){
        LedgerHandle lh = null;
        try {
            lh = bkMock.createLedger(BookKeeper.DigestType.MAC, "test".getBytes(StandardCharsets.UTF_8));
        } catch (BKException e) {
            e.printStackTrace();
        }
        return lh;
    }

    private void createNewEntry(){
        try {
            LedgerHandle lh =  createNewLedger();
            this.ledgerIdMatching = lh.getId();
            // test data
            this.expectedValue = "test".getBytes();
            int entrySize = HEADER_SIZE + this.expectedValue.length;
            this.entryIdMatching = 0L;

            ByteBuf entry = Unpooled.buffer(entrySize);
            entry.writeLong(this.ledgerIdMatching);
            entry.writeLong(this.entryIdMatching);
            entry.writeInt(entrySize);
            entry.writeBytes(this.expectedValue);

            lh.addEntry(entry.array());
            this.locationMatching = entryLogger.addEntry(this.ledgerIdMatching,entry,true);

        }catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void test_readEntry(){
        Exception exception = null;
        ByteBuf result = null;
        try{
            result = entryLogger.readEntry(this.ledgerId,this.entryId,this.location);
        }catch(IOException | IllegalArgumentException e){
            exception = e;
        }
        if(isExceptionExpected)
            Assert.assertNotNull(exception);
        else{
            Assert.assertNull(exception);
            byte[] all_data = new byte[HEADER_SIZE + this.expectedValue.length];
            result.readBytes(all_data);

            byte[] final_data = new byte[this.expectedValue.length];
            for(int i = HEADER_SIZE; i < HEADER_SIZE + this.expectedValue.length; i++){
                final_data[i-HEADER_SIZE] = all_data[i];
            }
            String expected = new String(this.expectedValue);
            String actual = new String(final_data, Charset.defaultCharset());
            Assert.assertEquals(expected,actual);
        }
    }

    @AfterClass
    public static void tearDown(){
        try {
            entryLogger.shutdown();
            bkMock.close();
            for(File dir: dirs){
                FileUtils.deleteDirectory(dir);
            }
        } catch (IOException | BKException | InterruptedException e) {
            e.printStackTrace();
            //Assert.fail();
        }finally {
            dirs.clear();
        }
    }

    private enum IdType{
        INVALID,
        MATCHING,
        NOT_MATCHING,
        //Seconda iterazione Jacoco
        OVERSIZE
    }

}


