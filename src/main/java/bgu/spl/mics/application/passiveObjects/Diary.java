package bgu.spl.mics.application.passiveObjects;


import java.util.concurrent.atomic.AtomicInteger;

/**
 * Passive data-object representing a Diary - in which the flow of the battle is recorded.
 * We are going to compare your recordings with the expected recordings, and make sure that your output makes sense.
 * <p>
 * Do not add to this class nothing but a single constructor, getters and setters.
 */
public class Diary {
private int totalAttacks = 0;
private long HanSoloFinish;
private long C3POFinish;
private long R2D2Deactivate;
private long LeiaTerminate;
private long HanSoloTerminate;
private long C3POTerminate;
private long R2D2Terminate;
    private long LandoTerminate;
    private static Diary instance = null;
    private static Object attackLock = new Object();

    public long getHanSoloFinish() {
        return HanSoloFinish;
    }

    public long getC3POFinish() {
        return C3POFinish;
    }

    public long getR2D2Deactivate() {
        return R2D2Deactivate;
    }

    public long getLeiaTerminate() {
        return LeiaTerminate;
    }

    public long getHanSoloTerminate() {
        return HanSoloTerminate;
    }

    public long getC3POTerminate() {
        return C3POTerminate;
    }

    public long getR2D2Terminate() {
        return R2D2Terminate;
    }

    public long getLandoTerminate() {
        return LandoTerminate;
    }



// To get those timestamps, simply use System.currentTimeMillis().
// Each timestamp is recorded by the specified name, e.g. only C3PO is allowed to
// set the value of C3POFinish. The totalAttacks member is recorded only by
// HanSolo or C3PO.


    private Diary() {}

    public static Diary getInstance(){
        if(instance == null)
            instance = new Diary();
        return instance;
    }

    public int getTotalAttacks() {
        return totalAttacks;
    }

    public void addAttack(){
        synchronized (attackLock) {
            this.totalAttacks++;
        }
    }


    public void setHanSoloFinish(long hanSoloFinish) {
        HanSoloFinish = hanSoloFinish;
    }

    public void setC3POFinish(long c3POFinish) {
        C3POFinish = c3POFinish;
    }

    public void setR2D2Deactivate(long r2D2Deactivate) {
        R2D2Deactivate = r2D2Deactivate;
    }

    public void setLeiaTerminate(long leiaTerminate) {
        LeiaTerminate = leiaTerminate;
    }

    public void setHanSoloTerminate(long hanSoloTerminate) {
        HanSoloTerminate = hanSoloTerminate;
    }

    public void setC3POTerminate(long c3POTerminate) {
        C3POTerminate = c3POTerminate;
    }

    public void setR2D2Terminate(long r2D2Terminate) {
        R2D2Terminate = r2D2Terminate;
    }

    public void setLandoTerminate(long landoTerminate) {
        LandoTerminate = landoTerminate;
    }

    public void resetNumberAttacks() {
        totalAttacks = 0;
    }
}