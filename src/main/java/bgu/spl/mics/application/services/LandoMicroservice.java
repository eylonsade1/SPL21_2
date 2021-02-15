package bgu.spl.mics.application.services;

import bgu.spl.mics.MicroService;
import bgu.spl.mics.application.messages.BombDestroyerEvent;
import bgu.spl.mics.application.messages.DeactivationEvent;
import bgu.spl.mics.application.messages.TerminationBroadcast;
import bgu.spl.mics.application.passiveObjects.Diary;

/**
 * LandoMicroservice
 * You can add private fields and public methods to this class.
 * You MAY change constructor signatures and even add new public constructors.
 */
public class LandoMicroservice extends MicroService {
    private long duration;

    public LandoMicroservice(long duration) {
        super("Lando");
        this.duration = duration;
    }

    @Override
    protected void initialize() {
        subscribeEvent(BombDestroyerEvent.class,
                message -> {
                    try {
                        Thread.sleep(duration);
                    } catch (Exception e) {
                    }
                    complete(message, true);
                  
                    sendBroadcast(new TerminationBroadcast());
                });

        subscribeBroadcast(TerminationBroadcast.class,
                message -> {
                    long terminate = System.currentTimeMillis();
                    Diary diary = Diary.getInstance();
                    diary.setLandoTerminate(terminate);
                    terminate();

                });

    }
}
