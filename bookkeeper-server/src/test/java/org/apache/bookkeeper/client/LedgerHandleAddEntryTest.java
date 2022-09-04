package org.apache.bookkeeper.client;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.*;

@RunWith(value = Parameterized.class)
public class LedgerHandleAddEntryTest extends LedgerHandleTest{

    private byte[] data;
    private int offset;
    private int length;
    private boolean isExceptionExpected;


    public LedgerHandleAddEntryTest(byte[] data, int offset, int length, boolean isExceptionExpected) {
        super();
        configure(data,offset,length,isExceptionExpected);
    }

    private void configure(byte[] data, int offset, int length, boolean isExceptionExpected){
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.isExceptionExpected = isExceptionExpected;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {      new byte[0],                             -1, -1, true},
                {      new byte[0],                             -1,  0, true},
                {      new byte[0],                             -1,  1, true},
                {      new byte[0],                              0,  0, false},
                {      new byte[0],                              0, -1, true},
                {      new byte[0],                              0,  1, true},
                {      new byte[0],                              1, -1, true},
                {      new byte[0],                              1,  0, true},
                {      "test".getBytes(StandardCharsets.UTF_8), -1, -1, true},
                {      "test".getBytes(StandardCharsets.UTF_8), -1,  0, true},
                {      "test".getBytes(StandardCharsets.UTF_8), -1,  5, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  0, -1, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  0,  0, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  0,  3, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  0,  4, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  0,  5, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  3, -1, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  3,  0, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  3,  1, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  3,  2, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  4, -1, true},
                //{      "test".getBytes(StandardCharsets.UTF_8),  4,  0, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  4,  1, true},

        });
    }

    @Test
    public void addEntryWithClosedLedgerTest(){
        closeLedgerHandle();
        try{
            ledgerHandle.addEntry(this.data, this.offset, this.length);
            Assert.fail();
        } catch (BKException | InterruptedException | ArrayIndexOutOfBoundsException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void addEntryTest(){
        Exception exception = null;
        try{
            long result = ledgerHandle.addEntry(this.data, this.offset, this.length);
            Assert.assertEquals(0L, result);
        }catch(InterruptedException | BKException | ArrayIndexOutOfBoundsException e){
            exception = e;
        }
        if(isExceptionExpected)
            Assert.assertNotNull(exception);
        else {
            Assert.assertNull(exception);
        }
    }



}
