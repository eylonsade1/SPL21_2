package bgu.spl.mics.application.passiveObjects;
import bgu.spl.mics.MessageBusImpl;

import java.util.*;

/**
 * Passive object representing the resource manager.
 * <p>
 * This class must be implemented as a thread-safe singleton.
 * You must not alter any of the given public methods of this class.
 * <p>
 * You can add ONLY private methods and fields to this class.
 */
public class Ewoks {

    private Vector <Ewok> ewoks;
    private static Ewoks instance = null;

    private Ewoks(int ewoksNum) {
        ewoks = new Vector<>();
        for (int i = 0; i < ewoksNum; i++)
            ewoks.add(new Ewok(i+1));
    }

    public boolean getEwoks(List<Integer> serial){
        for(int i: serial) {
            Ewok e = ewoks.elementAt(i - 1);
            try {
                e.acquire();
            }
            catch(Exception e1){
                for(int j = 0; j < i; j++) {
                    Ewok f = ewoks.elementAt(j - 1);
                    f.release();

                }
                return false;
            }
        }
        return true;
    }

    public static Ewoks getInstance(int ewoksNum) {
        if (instance == null)
            instance = new Ewoks(ewoksNum);
        return instance;
    }

    public void releaseEwoks (List<Integer> serial) {
        for (int i : serial) {
            Ewok e = ewoks.elementAt(i - 1);
            e.release();
        }
    }

    public void resetEwoks () {
        instance = null;
    }
}
