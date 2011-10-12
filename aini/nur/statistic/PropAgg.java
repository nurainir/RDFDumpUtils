/**
 * Counting number of distinct subject,object,blanknode, literal, uri and triples of each properties
 * @author Nur Aini Rakhmawati
 * @since Oct 5, 2008
 * @return Fields("predicate", "propsub", "triples" ) 
 */

package aini.nur.statistic;

import java.util.HashSet;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class PropAgg extends BaseOperation<PropAgg.Context> implements Aggregator<PropAgg.Context> {
	
	 public static class Context {
		    int subjectcount =0;
		    String lastsubject="-";
		     int triples=0;
		     HashSet<String> objectset = new HashSet<String>();
		     int uricount =0;
			   int literalcount=0;
			   int blanknodecount =0;
		     
		  }

	/**
	 * 
	 */
	public PropAgg() {
		 super( 1, new Fields( "propsub", "propob","triples" ,"blanknode","literal","uri") );

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
	
	    String object = arguments.getString("object");
		context.objectset.add(object);
	    	    
		if(object.startsWith("_:"))
			context.blanknodecount++;
		else if (object.startsWith("<http:"))
			context.uricount++;
		else
			context.literalcount++;

		
	   context.triples++;
	    	
		
	}

	@Override
	public void complete(FlowProcess arg0, AggregatorCall<Context> aggregatorCall) {
		final Context context = aggregatorCall.getContext();
	    final Tuple result = new Tuple();

	    result.add(context.subjectcount);
	    result.add(context.objectset.size());
	    result.add(context.triples);
		   result.add(context.blanknodecount);
		   result.add(context.literalcount);
		   result.add(context.uricount);
		 
	    aggregatorCall.getOutputCollector().add(result);
		
	}

	@Override
	public void start(FlowProcess arg0, AggregatorCall<Context> arg1) {
		arg1.setContext(new Context());
		
	}

}
