package storm.starter.trident.project.countmin.state;

import storm.starter.trident.project.countmin.Data;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;

import java.util.List;
import java.util.ArrayList;
import java.util.PriorityQueue;

/**
 *@author: Preetham MS (pmahish@ncsu.edu)
 */

public class CountMinSketchUpdater extends BaseStateUpdater<CountMinSketchState> {
	
	public static PriorityQueue<Data> queue;

	public CountMinSketchUpdater(PriorityQueue<Data> queue){
		this.queue=queue;
		
	}
	
    public void updateState(CountMinSketchState state, List<TridentTuple> tuples, TridentCollector collector) {
        List<Long> ids = new ArrayList<Long>();
        List<String> locations = new ArrayList<String>();
        for(TridentTuple t: tuples) {
            //ids.add(t.getLong(0));
            //locations.add(t.getString(1));

            state.add(t.getString(0),1);
            long count=state.estimateCount(t.getString(0));
            Data d=new Data(t.getString(0),count);
            if(queue.size()<5){
            	boolean flag=false;
            	for(Data temp:queue){
        			if(temp.message.equals(d.message)){
        				queue.remove(temp);
        				queue.add(d);
        				flag=true;
        				break;
        			}
        		}
        		if(!flag){
        		//queue.poll();
        		queue.add(d);
        		}
            }
            else{
            	boolean flag=false;
            	if(queue.peek().priority<d.priority){
            		for(Data temp:queue){
            			if(temp.message.equals(d.message)){
            				queue.remove(temp);
            				queue.add(d);
            				flag=true;
            				break;
            			}
            		}
            		if(!flag){
            		queue.poll();
            		queue.add(d);
            		}
            	}
            	
            }
            
        }
        
    }
}
