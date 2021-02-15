package bgu.spl.mics.application.messages;
import bgu.spl.mics.Event;
import bgu.spl.mics.Future;

import java.util.List;

public class AttackEvent implements Event<Boolean> {
    private Future future = new Future();
    final List<Integer> resources;
    final int duration;

    public List<Integer> getResources() {
        return resources;
    }

    public int getDuration() {
        return duration;
    }

    public AttackEvent(int duration, List<Integer> resources){
        this.duration = duration;
        this.resources = resources;
    }

}
