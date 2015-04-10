package storm.starter.trident.project.filters;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import storm.starter.trident.project.BloomFilter.BloomCalculations;
import storm.starter.trident.project.BloomFilter.BloomFilter;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class FilterStopWords extends BaseFilter {
	public static BloomFilter bf;
	public static BloomCalculations.BloomSpecification spec = BloomCalculations.computeBucketsAndK(0.0001);
    static final int ELEMENTS = 10000;
    
    public void initBF(){
    	bf = new BloomFilter(ELEMENTS, spec.bucketsPerElement);
    	
    	FileInputStream fstream = null;
		try {
			fstream = new FileInputStream("data/stop-words.txt");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

    	String stopWord;

    	//Read File Line By Line
    	try {
			while ((stopWord = br.readLine()) != null)   {
			  // Print the content on the console
			  bf.add(stopWord);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	//Close the input stream
    	try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
	
	
	public FilterStopWords() {
	
	}
	public boolean isKeep(TridentTuple tuple) {
	for(Object o: tuple) {
	if(bf.isPresent(o.toString())) return false;
	}
	return true;
	}

}
