/**
 * Counting number of distinct object and triples of each properties
 * @author Nur Aini Rakhmawati
 * @since Oct 5, 2011
 * @return Fields("predicate", "propob" ) 
 */

package aini.nur.statistic;


import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ObjAgg extends BaseOperation<ObjAgg.Context> implements Aggregator<ObjAgg.Context> {
	
	 public static class Context {
		   
		   int objectcount =0;
		   String lastobject="-";
	
		  }

	public ObjAgg() {
		 super( 1, new Fields( "propob" ) );
	}

	@Override
	public void aggregate(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {
		final TupleEntry arguments = aggregatorCall.getArguments();
	    final Context context = aggregatorCall.getContext(); 
		if(!context.lastobject.equalsIgnoreCase(arguments.getString("object")))
	    {
	    	context.objectcount+=1;
	    	context.lastobject=arguments.getString("object");
	    }
		
	}

	@Override
	public void complete(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {
		final Context context = aggregatorCall.getContext();
	    final Tuple result = new Tuple();
	   result.add(context.objectcount);
	   aggregatorCall.getOutputCollector().add(result);
	}

	@Override
	public void start(FlowProcess arg0, AggregatorCall<Context> arg1) {
		arg1.setContext(new Context());
		
	}
}
