package bgu.spl.mics.application.services;


import bgu.spl.mics.MicroService;
import bgu.spl.mics.application.messages.AttackEvent;
import bgu.spl.mics.application.messages.TerminationBroadcast;
import bgu.spl.mics.application.passiveObjects.Diary;
import bgu.spl.mics.application.passiveObjects.Ewoks;

import java.util.List;

/**
 * HanSoloMicroservices is in charge of the handling {@link AttackEvent}.
 * This class may not hold references for objects which it is not responsible for:
 * {@link AttackEvent}.
 *
 * You can add private fields and public methods to this class.
 * You MAY change constructor signatures and even add new public constructors.
 */
public class HanSoloMicroservice extends MicroService {

    public HanSoloMicroservice() {
        super("Han");
    }


    @Override
    protected void initialize() {
        subscribeEvent(AttackEvent.class,
                message -> {
                    List<Integer> ewok = message.getResources();
                    Ewoks ewoks = Ewoks.getInstance(0);
                    boolean gotEwok = false;
                    while (!gotEwok)
                        gotEwok = ewoks.getEwoks(ewok);
                    int duration = message.getDuration();
                    try {
                        Thread.sleep(duration);
                    } catch (Exception e) {
                    }
                    ewoks.releaseEwoks(ewok);
                    Diary diary = Diary.getInstance();
                    diary.setHanSoloFinish(System.currentTimeMillis());
                    diary.addAttack();
                    complete(message, true);
                });
        subscribeBroadcast(TerminationBroadcast.class,
                message -> {
                    long terminate = System.currentTimeMillis();
                    Diary diary = Diary.getInstance();
                    diary.setHanSoloTerminate(terminate);
                    terminate();

                });


    }
}
