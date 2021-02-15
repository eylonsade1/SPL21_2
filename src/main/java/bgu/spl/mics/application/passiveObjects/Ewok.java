package bgu.spl.mics.application.passiveObjects;

import java.util.concurrent.Semaphore;

/**
 * Passive data-object representing a forest creature summoned when HanSolo and C3PO receive AttackEvents.
 * You must not alter any of the given public methods of this class.
 * <p>
 * You may add fields and methods to this class as you see fit (including public methods).
 */
public class Ewok {
     int serialNumber;
	 boolean available;
	private Semaphore lock = new Semaphore(1,true);



    public Ewok(int serialNumber) {
        this.serialNumber = serialNumber;
        this.available = true;
    }

    /**
     * Acquires an Ewok
     */
    public void acquire() throws Exception {
            lock.acquire();
        if(!this.available)
            throw new Exception("This ewok is not available!");
        else
	    	this.available = false;
    }

    /**
     * release an Ewok
     */
    public void release() {
        lock.release();
        this.available = true;
    }
}
