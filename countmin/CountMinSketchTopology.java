package storm.starter.trident.project.countmin;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Values;

import java.util.*;

import storm.trident.operation.builtin.Count;
import storm.starter.trident.project.countmin.state.CountMinSketchStateFactory;
import storm.starter.trident.project.countmin.state.CountMinQuery;
import storm.starter.trident.project.countmin.state.CountMinSketchUpdater;
import storm.starter.trident.project.filters.FilterStopWords;
import storm.starter.trident.project.filters.PrintFilter;
import storm.starter.trident.project.functions.HeavyHitter;
import storm.starter.trident.project.functions.ParseTweet;
import storm.starter.trident.project.functions.SentenceBuilder;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.starter.trident.tutorial.functions.SplitFunction;
import storm.starter.trident.tutorial.functions.ToLowerCaseFunction;

/**
 * @author: Preetham MS (pmahish@ncsu.edu)
 */

public class CountMinSketchTopology {
	public static PriorityQueue<Data> queue;

	public static StormTopology buildTopology(String[] args, LocalDRPC drpc) {

		// Create a priority queue to maintain the top 5 highest frequency words
		// in the stream. [Implementing Top-K where K=5]
		queue = new PriorityQueue<Data>();

		TridentTopology topology = new TridentTopology();

		int width = 100;
		int depth = 20;
		int seed = 10;

		// Twitter's account credentials passed as args
		String consumerKey = args[0];
		String consumerSecret = args[1];
		String accessToken = args[2];
		String accessTokenSecret = args[3];

		// Twitter topic of interest.
		String[] arguments = args.clone();
		String[] topicWords = Arrays
				.copyOfRange(arguments, 4, arguments.length);

		// Initialize the Bloom Filter for filtering stop words. Include file
		// "stop-words.txt" in the "/data/" directory.
		FilterStopWords bf = new FilterStopWords();
		bf.initBF();

		// Create Twitter's spout
		TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey,
				consumerSecret, accessToken, accessTokenSecret, topicWords);

		TridentState countMinDBMS = topology
				.newStream("tweets", spoutTweets)
				.parallelismHint(1)
				.each(new Fields("tweet"), new ParseTweet(),
						new Fields("text", "tweetId", "user"))
				.each(new Fields("text"), new SentenceBuilder(),
						new Fields("sentence"))
				.each(new Fields("sentence"), new Split(), new Fields("words"))
				// Convert each word of the tweet to lower case before matching
				// it with the stop words list of the bloom filter.
				.each(new Fields("words"), new ToLowerCaseFunction(),
						new Fields("lowercaseWords"))
				// Filter stop words before inserting it into the Count Min
				// sketch.
				.each(new Fields("lowercaseWords"), new FilterStopWords())
				
				// Logic to update the priority queue with Top-K words is in CountMinSketchUpdater().
				.partitionPersist(
						new CountMinSketchStateFactory(depth, width, seed),
						new Fields("lowercaseWords"),
						new CountMinSketchUpdater(queue));

		
		
		topology.newDRPCStream("get_count", drpc)
				.each(new Fields("args"), new Split(), new Fields("query"))
				.stateQuery(countMinDBMS, new Fields("query"),
						new CountMinQuery(), new Fields("count"))
				.project(new Fields("query", "count"))

		;

		return topology.build();

	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setDebug(false);
		conf.setMaxSpoutPending(10);

		LocalCluster cluster = new LocalCluster();
		LocalDRPC drpc = new LocalDRPC();
		cluster.submitTopology("get_count", conf, buildTopology(args, drpc));
		String query = " ";

		for (int i = 0; i < 5; i++) {
			// Construct the query string using the words in the priority queue
			// which maintains the Top-K most frequent words in the stream
			// and query this for its frequency from the Count Min sketch.
			for (Data temp : queue) {
				query = query + temp.message + " ";
			}

			System.out.println("DRPC RESULT:"
					+ drpc.execute("get_count", query));
			query = " ";

			Thread.sleep(5000);
		}

		System.out.println("STATUS: OK");
		// cluster.shutdown();
		// drpc.shutdown();
	}
}
