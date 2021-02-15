package bgu.spl.mics;

import bgu.spl.mics.application.messages.AttackEvent;
import bgu.spl.mics.application.messages.DeactivationEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.fail;


public class MessageBusImplTest {
    MessageBus bus;
    MicroService service;
    Event event;
    Broadcast broadcast;

    @Before
    public void set() {
        bus = MessageBusImpl.getInstance();
    //    event = new AttackEvent();
        broadcast = Mockito.mock(Broadcast.class);
           service = new MicroService("test") {

        @Override
        protected void initialize() {
            subscribeBroadcast(Broadcast.class, (Broadcast r) -> {
                return;
            });
            try {
                bus.awaitMessage(service);

            } catch (InterruptedException e) {
                fail();
                e.printStackTrace();
            }
        }
    };

}


    @Test
    public void subscribeEvent() throws Exception {
        try {
            bus.subscribeEvent(AttackEvent.class, service);
            fail("Subscribe fail");
        } catch (Exception e) {
            bus.register(service);
            bus.subscribeEvent(AttackEvent.class, service);
            bus.unregister(service);
        }

    }

    @Test
    public void subscribeBroadcast() throws Exception {
        try {
            bus.subscribeBroadcast(Broadcast.class, service);
            fail("Subscribe fail");
        } catch (Exception e) {
            bus.register(service);
            bus.subscribeBroadcast(Broadcast.class, service);
            bus.unregister(service);
        }

    }

    @Test
    public void complete() throws Exception {
        String str = "someResult";
        try {
            bus.complete(event, str);
            fail("Complete fail");
        } catch (IllegalArgumentException e) {
            bus.register(service);
            bus.subscribeEvent(AttackEvent.class, service);
            bus.sendEvent(event);
            bus.complete(event, str);
            bus.unregister(service);
        }
    }

    @Test
    public void sendBroadcast() {
        try {
            bus.sendBroadcast(broadcast);
            fail("Send fail");
        } catch (Exception e) {
            bus.register(service);
            bus.subscribeBroadcast(broadcast.getClass(), service);
            bus.sendBroadcast(broadcast);
            bus.unregister(service);
        }
    }

    @Test
    public void sendEvent() {
        event = new DeactivationEvent();
        try {
            bus.sendEvent(event);
            fail("Send fail");
        } catch (IllegalArgumentException e) {
            bus.register(service);
            bus.subscribeEvent(DeactivationEvent.class, service);
            bus.sendEvent(event);
            bus.unregister(service);
        }
    }

    @Test
    public void register() throws Exception {
        bus.register(service);
        try {
            bus.register(service);
            fail("register fail");
        } catch (IllegalArgumentException e) {
        }
        bus.unregister(service);
    }

    @Test
    public void awaitMessage() throws Exception {
        Thread thread = new Thread(service);
        thread.join();
        Thread.sleep(200);
        bus.sendBroadcast(broadcast);
        Thread.sleep(1000);
        if (thread.isAlive())
            fail("Thread is still alive");
    }
}