package bgu.spl.mics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;


public class FutureTest {

    private Future<String> future;

    @BeforeEach
    public void setUp() {
        future = new Future<>();
    }

    @Test
    public void testResolve() {
        String str = "someResult";
        future.resolve(str);
        assertTrue(future.isDone());
        assertTrue(str.equals(future.get()));
    }

    @Test
    public void isDone() throws Exception {
        String str = "someResult";
        assertFalse(future.isDone());
        future.resolve(str);
        assertTrue(future.isDone());
    }

    @Test
    public void get1() throws Exception {
        String str = "someResult";
        future.resolve(str);
        String result = future.get();
        assertEquals(result, str);
    }

    @Test
    public void get2() throws Exception{
        assertFalse(future.isDone());
        future.get(100,TimeUnit.MILLISECONDS);
        assertFalse(future.isDone());
        future.resolve("someResult");
        assertEquals(future.get(100,TimeUnit.MILLISECONDS),"someResult");
    }

}
