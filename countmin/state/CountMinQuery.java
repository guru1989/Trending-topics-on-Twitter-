//import CountMinSketchState;
package storm.starter.trident.project.countmin.state;

import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import java.util.List;
import java.util.ArrayList;
import backtype.storm.tuple.Values;


/**
 *@author: Preetham MS (pmahish@ncsu.edu)
 */


public class CountMinQuery extends BaseQueryFunction<CountMinSketchState, String> {
    public List<String> batchRetrieve(CountMinSketchState state, List<TridentTuple> inputs) {
        List<String> ret = new ArrayList();
        for(TridentTuple input: inputs) {
            ret.add(String.valueOf(state.estimateCount(input.getString(0))));
        }
        return ret;
    }

    public void execute(TridentTuple tuple, String count, TridentCollector collector) {
        collector.emit(new Values(count));
    }    
}
