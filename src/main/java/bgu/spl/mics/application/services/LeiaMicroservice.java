package bgu.spl.mics.application.services;

import bgu.spl.mics.Future;
import bgu.spl.mics.MicroService;
import bgu.spl.mics.application.messages.AttackEvent;
import bgu.spl.mics.application.messages.BombDestroyerEvent;
import bgu.spl.mics.application.messages.DeactivationEvent;
import bgu.spl.mics.application.messages.TerminationBroadcast;
import bgu.spl.mics.application.passiveObjects.Attack;
import bgu.spl.mics.application.passiveObjects.Diary;

import java.util.Vector;

/**
 * LeiaMicroservices Initialized with Attack objects, and sends them as  {@link AttackEvent}.
 * This class may not hold references for objects which it is not responsible for:
 * {@link AttackEvent}.
 * <p>
 * You can add private fields and public methods to this class.
 * You MAY change constructor signatures and even add new public constructors.
 */
public class LeiaMicroservice extends MicroService {
    private Attack[] attacks;
    private Vector<Future> futures;

    public LeiaMicroservice(Attack[] attacks) {
        super("Leia");
        this.attacks = attacks;
        this.futures = new Vector<>();
    }

    @Override
    protected void initialize() {

        for (Attack attack : attacks) {
            try {
                Thread.sleep(2000);
            } catch (Exception e) {
            }
            futures.add(sendEvent(new AttackEvent(attack.getDuration(), attack.getSerials())));
        }
        subscribeBroadcast(TerminationBroadcast.class,
                message -> {
                    long terminate = System.currentTimeMillis();
                    Diary diary = Diary.getInstance();

                    diary.setLeiaTerminate(terminate);
                    terminate();
                });


        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception e1) {
            }
        }
        Future deactivate = sendEvent(new DeactivationEvent());
        try {
            while (!deactivate.isDone()) {
                wait();
            }
        } catch (Exception e) {
        }
        sendEvent(new BombDestroyerEvent());
    }
}
