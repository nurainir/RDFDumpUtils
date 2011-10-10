/**
 * Counting number of distinct subject and triples of each properties
 * @author Nur Aini Rakhmawati
 * @since Oct 5, 2011
 * @return Fields("predicate", "propsub", "triples" ) 
 */

package aini.nur.statistic;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class SubAgg extends BaseOperation<SubAgg.Context> implements Aggregator<SubAgg.Context> {
	
	 public static class Context {
		    int subjectcount =0;
		    String lastsubject="-";
		     int triples=0;
		  }

	/**
	 * 
	 */
	public SubAgg() {
		 super( 1, new Fields( "propsub", "triples" ) );

	}

	@Override
	public void aggregate(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {
		final TupleEntry arguments = aggregatorCall.getArguments();
	    final Context context = aggregatorCall.getContext();
	    if(!context.lastsubject.equalsIgnoreCase(arguments.getString("subject")))
	    {
	    	context.subjectcount+=1;
	    	context.lastsubject=arguments.getString("subject");
	    }
	
	   context.triples++;
	    	
		
	}

	@Override
	public void complete(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {
		final Context context = aggregatorCall.getContext();
	    final Tuple result = new Tuple();

	    result.add(context.subjectcount);
	    result.add(context.triples);
	    aggregatorCall.getOutputCollector().add(result);
		
	}

	@Override
	public void start(FlowProcess arg0, AggregatorCall<Context> arg1) {
		arg1.setContext(new Context());
		
	}

}
