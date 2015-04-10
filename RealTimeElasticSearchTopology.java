package storm.starter.trident.project;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.tuple.TridentTuple;
import storm.trident.testing.MemoryMapState;
import storm.trident.state.StateFactory;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;

import static org.elasticsearch.node.NodeBuilder.*;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.node.Node;

import com.github.tlrx.elasticsearch.test.EsSetup;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteIndex;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import storm.starter.trident.project.BloomFilter.BloomCalculations;
import storm.starter.trident.project.BloomFilter.BloomFilter;
import storm.starter.trident.project.countmin.state.CountMinSketchStateFactory;
import storm.starter.trident.project.countmin.state.CountMinSketchUpdater;
// Local spouts, functions and filters
import storm.starter.trident.project.functions.TweetBuilder;
import storm.starter.trident.project.functions.DocumentBuilder;
import storm.starter.trident.project.functions.ExtractDocumentInfo;
import storm.starter.trident.project.functions.ExtractSearchArgs;
import storm.starter.trident.project.functions.CreateJson;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.starter.trident.project.functions.ParseTweet;
import storm.starter.trident.project.filters.FilterStopWords;
import storm.starter.trident.project.filters.Print;
import storm.starter.trident.project.filters.PrintFilter;
import storm.starter.trident.project.functions.Tweet;
import storm.starter.trident.project.functions.SentenceBuilder;
import storm.starter.trident.tutorial.functions.ToLowerCaseFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.github.fhuss.storm.elasticsearch.mapper.impl.DefaultTupleMapper;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.state.ESIndexMapState;
import com.github.fhuss.storm.elasticsearch.state.ESIndexUpdater;
import com.github.fhuss.storm.elasticsearch.state.ESIndexState;
import com.github.fhuss.storm.elasticsearch.state.QuerySearchIndexQuery;
import com.github.fhuss.storm.elasticsearch.ClientFactory.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
* This topology shows how to build ElasticSearch engine for a stream made of
* fake tweets and how to query it, using DRPC calls. 
* This example should be intended as
* an example of {@link TridentState} custom implementation.
* Modified by YOUR NAME from original author:
* @author Nagiza Samatova (samatova@csc.ncsu.edu)
* @author Preetham Srinath (pmahish@ncsu.edu)
*/
public class RealTimeElasticSearchTopology {
	
	
	
	public static StormTopology buildTopology(String[] args, Settings settings,
			LocalDRPC drpc) throws IOException {

		TridentTopology topology = new TridentTopology();

		ESIndexMapState.Factory<Tweet> stateFactory = ESIndexMapState
				.nonTransactional(
						new ClientFactory.NodeClient(settings.getAsMap()),
						Tweet.class);

		TridentState staticState = topology
				.newStaticState(new ESIndexState.Factory<Tweet>(new NodeClient(
						settings.getAsMap()), Tweet.class));

		/**
		 * Create spout(s)
		 **/
		// Twitter's account credentials passed as args
		String consumerKey = args[0];
		String consumerSecret = args[1];
		String accessToken = args[2];
		String accessTokenSecret = args[3];

		// Twitter topic of interest. arguments passed in my test run is "love"
		// & "hate" as the search queries are built based on these keywords.
		String[] arguments = args.clone();
		String[] topicWords = Arrays
				.copyOfRange(arguments, 4, arguments.length);

		// Create Twitter's spout
		TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey,
				consumerSecret, accessToken, accessTokenSecret, topicWords);

		topology.newStream("tweets", spoutTweets)
				.parallelismHint(1)
				.each(new Fields("tweet"), new ParseTweet(),
						new Fields("text", "tweetId", "user"))
				// inserting only text field of the tweet into the ES index ignoring other tweet fields for simplicity
				.each(new Fields("text"), new SentenceBuilder(),				
						new Fields("sentence"))
				.each(new Fields("sentence"), new DocumentBuilder(),
						new Fields("document"))
				.each(new Fields("document"), new ExtractDocumentInfo(),
						new Fields("id", "index", "type"))
				.groupBy(new Fields("index", "type", "id"))
				.persistentAggregate(stateFactory, new Fields("document"),
						new TweetBuilder(), new Fields("tweet"))
				.parallelismHint(1);

		/**
		 * Now use a DRPC stream to query the state where the tweets are stored.
		 * CRITICAL: DO NOT CHANGE "query", "indicies", "types" WHY:
		 * QuerySearchIndexQuery() has hard-coded these tags in its code:
		 * https://github.com/fhussonnois/storm-trident-elasticsearch/blob/13
		 * bd8203503a81754dc2a421accff216b665a11d
		 * /src/main/java/com/github/fhuss/
		 * storm/elasticsearch/state/QuerySearchIndexQuery.java
		 */

		topology.newDRPCStream("search_event", drpc)
				.each(new Fields("args"), new ExtractSearchArgs(),
						new Fields("query", "indices", "types"))
				.groupBy(new Fields("query", "indices", "types"))
				.stateQuery(staticState,
						new Fields("query", "indices", "types"),
						new QuerySearchIndexQuery(), new Fields("tweet"))
				.each(new Fields("tweet"), new FilterNull())
				.each(new Fields("tweet"), new CreateJson(), new Fields("json"))
				.project(new Fields("json"));

		return topology.build();
	}

    public static void main(String[] args) throws Exception {

		// Specify the name and the type of ES index 
		String index_name = new String();
		String index_type = new String();
		index_name = "my_index";
		index_type = "my_type";
	

		/**
		* Configure local cluster and local DRPC 
		***/
        Config conf = new Config();
		conf.setMaxSpoutPending(10);
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();

		/**
		* Configure ElasticSearch related information
		* Make sure that elasticsearch (ES) server is up and running
		* Check README.md file on how to install and run ES
		* Note that you can check if index exists and/or was created
		* with curl 'localhost:9200/_cat/indices?v'
		* and type: curl ‘http://127.0.0.1:9200/my_index/_mapping?pretty=1’
		* IMPORTANT: DO NOT CHANGE PORT TO 9200. IT MUST BE 9300 in code below.
		**/
		Settings settings = ImmutableSettings.settingsBuilder()
			.put("storm.elasticsearch.cluster.name", "elasticsearch")
			.put("storm.elasticsearch.hosts", "127.0.0.1:9300")
			.build();

		Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
    
		/** If you need more options, you can check test code:
		* https://github.com/tlrx/elasticsearch-test/blob/master/src/test/java/com/github/tlrx/elasticsearch/test/EsSetupTest.java
		* Remove else{} stmt if you do not want index to be deleted but rather updated
		* NOTE: Index files are stored by default under $HOME/elasticsearch-1.4.2/data/elasticsearch/nodes/0/indices
		**/
		EsSetup esSetup = new EsSetup(client);
		if (! esSetup.exists(index_name))
		{
			esSetup.execute(createIndex(index_name));
		}
		else
		{
			esSetup.execute(deleteIndex(index_name));
			esSetup.execute(createIndex(index_name));
		}

        cluster.submitTopology("state_drpc", conf, buildTopology(args, settings, drpc));

		System.out.println("STARTING DRPC QUERY PROCESSING");

	   /***
	   * TO DO FOR PROJECT 2: PART B:
	   *   3. Investigate Java APIs for ElasticSearch for various types of queries:
	   *		http://www.elasticsearch.com/guide/en/elasticsearch/client/java-api/current/index.html
	   *   4. Create at least 3 different query types besides termQuery() below
	   *		and test them with the DRPC execute().
	   *	    Example queries: matchQuery(), multiMatchQuery(), fuzzyQuery(), etc.
	   *   5. Can you thing of the type of indexing technologies ES might be using
	   *		to support efficient query processing for such query types: 
	   *		e.g., fuzzyQuery() vs. boolQuery().
	   ***/
	   
	   // Query1 is boolQuery which returns all tweets containing keyword "love" and not containing "hate"
       String query1 = QueryBuilders
       .boolQuery()
       .must(QueryBuilders.termQuery("text", "love"))
       .mustNot(QueryBuilders.termQuery("text", "hate"))
       .buildAsBytes().toUtf8();
       
       // Query2 is fuzzyQuery which returns all tweets matching keyword "lov" in a fuzzy manner. ["love" is also a match in this case]
       String query2 = QueryBuilders.fuzzyQuery("text", "lov").buildAsBytes().toUtf8();
	   
       // Query3 is termQuery which returns all tweets matching keyword "hate"
       String query3 = QueryBuilders.termQuery("text", "hate").buildAsBytes().toUtf8();
	   
       // Query4 is wildcardQuery which returns all tweets matching the regular expression "l?ve"
       String query4 = QueryBuilders.wildcardQuery("text", "l?ve").buildAsBytes().toUtf8();
	   
       // Query5 is prefixQuery which returns all tweets matching the prefix "hat"
       String query5 = QueryBuilders.prefixQuery("text", "hat").buildAsBytes().toUtf8();
	   
       String drpcResult;
       for (int i = 0; i < 5; i++) {
          drpcResult = drpc.execute("search_event", query1+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT: BOOL QUERY: " + drpcResult);
		  drpcResult = drpc.execute("search_event", query2+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT: FUZZY QUERY: " + drpcResult);
	      drpcResult = drpc.execute("search_event", query3+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT: TERM QUERY:  " + drpcResult);
          drpcResult = drpc.execute("search_event", query4+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT: WILDCARD QUERY:  " + drpcResult);
          drpcResult = drpc.execute("search_event", query5+" "+index_name+" "+index_type);
          System.out.println("DRPC RESULT: PREFIX QUERY:  " + drpcResult);
          Thread.sleep(4000);
       }

        System.out.println("STATUS: OK");

        //cluster.shutdown();
        //drpc.shutdown();
		//esSetup.terminate(); 
    }
}
