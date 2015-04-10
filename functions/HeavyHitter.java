package storm.starter.trident.project.functions;
import java.util.PriorityQueue;

import backtype.storm.tuple.Values;
import storm.starter.trident.project.countmin.Data;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class HeavyHitter extends BaseFunction{
	
	public PriorityQueue<Data> queue;
	public HeavyHitter(PriorityQueue<Data> queue){
		this.queue=queue;
		
	}
	@Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        
            if(queue.size() > 0) {
                collector.emit(new Values(queue.peek().message));
            }
        
    }

}
