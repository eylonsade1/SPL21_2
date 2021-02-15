package bgu.spl.mics.application.passiveObjects;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EwokTest {
    Ewok ewok;

    @Before
    public void setUp() throws Exception {
        ewok = new Ewok(1);
    }

    @Test
    public void acquire() throws Exception {
        assertTrue(ewok.available);
        ewok.acquire();
        assertFalse(ewok.available);
        try {
            ewok.acquire();
        } catch (Exception e) {
        }
    }

    @Test
    public void release() throws Exception {
        ewok.available = false;
        assertFalse(ewok.available);
        ewok.release();
        assertTrue(ewok.available);
        try {
            ewok.release();
        } catch (Exception e) {
        }
    }
}