package storm.starter.trident.project.functions;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import storm.starter.trident.project.functions.Tweet;
import com.google.common.collect.Lists;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.state.ESIndexMapState;
import org.elasticsearch.index.query.QueryBuilders;
//import org.junit.*;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;

/**
 * Default test class.
 *
 * @author fhussonnois
 */


   public class CreateJson extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            try {
                collector.emit(new Values(new ObjectMapper().writeValueAsString(tuple.getValue(0))));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

