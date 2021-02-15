package bgu.spl.mics.application.services;

import bgu.spl.mics.Callback;
import bgu.spl.mics.MicroService;
import bgu.spl.mics.application.messages.AttackEvent;
import bgu.spl.mics.application.messages.TerminationBroadcast;
import bgu.spl.mics.application.passiveObjects.Diary;
import bgu.spl.mics.application.passiveObjects.Ewoks;

import java.util.List;


/**
 * C3POMicroservices is in charge of the handling {@link AttackEvent}.
 * This class may not hold references for objects which it is not responsible for:
 * {@link AttackEvent}.
 * <p>
 * You can add private fields and public methods to this class.
 * You MAY change constructor signatures and even add new public constructors.
 */
public class C3POMicroservice extends MicroService {

    public C3POMicroservice() {
        super("C3PO");
    }

    @Override
    protected void initialize() {
        subscribeEvent(AttackEvent.class,
                message -> {
            List<Integer> ewok = message.getResources();
            Ewoks ewoks = Ewoks.getInstance(0);
            boolean gotEwok = false;
         try {
            while (!gotEwok) {
                gotEwok = ewoks.getEwoks(ewok);
            }
            int duration = message.getDuration();

                Thread.sleep(duration);
            } catch (Exception e) {
            }

            ewoks.releaseEwoks(ewok);

                    //    send time stamp to diary when finished all attacks in queue
            complete(message, true);
                    Diary diary = Diary.getInstance();
                    diary.setC3POFinish(System.currentTimeMillis());
                    diary.addAttack();
        });
        subscribeBroadcast(TerminationBroadcast.class,
                message -> {
                    long terminate = System.currentTimeMillis();
                    Diary diary = Diary.getInstance();
                    diary.setC3POTerminate(terminate);
                    terminate();

                });

    }
}
