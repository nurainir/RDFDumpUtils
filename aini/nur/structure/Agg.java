/**
 * List of instance , type of instance and predicate  
 * @author Nur Aini Rakhmawati
 * @since Oct 10, 2011
 * @return  Fields( "type" ,"predicateoftype") 
 */
package aini.nur.structure;


import java.util.HashSet;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class Agg extends BaseOperation<Agg.Context> implements Aggregator<Agg.Context> {
	

	public static class Context {
		   
		Set<String> typesOfCurrentSubject = new HashSet<String>();
		Set<String> predsOfCurrentSubject = new HashSet<String>();
	
		  }
	

	public Agg() {
		 super( 1, new Fields( "type" ,"predicateoftype") );
	}

	
	@Override
	public void aggregate(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {
		final TupleEntry arguments = aggregatorCall.getArguments();
	    final Context context = aggregatorCall.getContext(); 
	    String predicate = arguments.getString("predicate");
	    String object = arguments.getString("object");
	    
	    if(!predicate.equalsIgnoreCase("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"))
		 {
	    	context.predsOfCurrentSubject.add(predicate);
		}
	    else
	    	context.typesOfCurrentSubject.add(object);
		
	}

	@Override
	public void complete(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {
		final Context context = aggregatorCall.getContext();
		
		
		
		for (String type : context.typesOfCurrentSubject)
		{
			// if instance only have one triple with predicate rdf type
			if (context.predsOfCurrentSubject.size()==0)
			{
				final Tuple result = new Tuple();
				result.add(type);
				result.add("-");
				
				 aggregatorCall.getOutputCollector().add(result);
			}
			else
			{
			for (String pred : context.predsOfCurrentSubject)
			{
				final Tuple result = new Tuple();
				result.add(type);
				result.add(pred);
				
				 aggregatorCall.getOutputCollector().add(result);
			}
			}
		}
		
	}

	@Override
	public void start(FlowProcess arg0, AggregatorCall<Context> arg1) {
		arg1.setContext(new Context());
		
	}


}
