/**
 * aini.nur.field-UniqueNumber.java
 * Assigning unique id for each node (subject or object)
 * @author : Nur Aini Rakhmawati
 * @since  : 14 Oct 2011
 */
package aini.nur.field;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class UniqueNumber extends BaseOperation implements Aggregator {

	int id =0;
	
	public UniqueNumber() {
		super( 1, new Fields( "id"));
	}

	@Override
	public void aggregate(FlowProcess arg0, AggregatorCall arg1) {
		
		
	}

	@Override
	public void complete(FlowProcess arg0, AggregatorCall aggregatorCall) {
		   final Tuple result = new Tuple();
		   id++;
		   result.add(id);
		   aggregatorCall.getOutputCollector().add(result);
		
	}

	@Override
	public void start(FlowProcess arg0, AggregatorCall arg1) {
		// TODO Auto-generated method stub
		
	}

}
