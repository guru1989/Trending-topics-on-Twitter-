package storm.starter.trident.project.filters;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Print filter for printing Trident tuples. 
 * used in debugging
 *
 * @author preems
 */

public class PrintFilter extends BaseFilter {

	String prefix="";

	public PrintFilter(String prefix) {
		this.prefix=prefix;
	}

	public PrintFilter() {

	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		System.out.println(prefix+tuple);
		return true;
	}
}
