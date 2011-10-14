/**
 * aini.nur.metis-SplitSubObj.java
 * splitting subject and object in different tuple and reserving each other
 * @author : Nur Aini Rakhmawati
 * @since  : 14 Oct 2011
 */
package aini.nur.metis;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class SplitSubObj extends BaseOperation implements Function {

	@Override
	public void operate(FlowProcess arg0, FunctionCall functionCall) {
		TupleEntry arguments = functionCall.getArguments();
		String subject = arguments.getString("subject");
		String object = arguments.getString("object");
		Tuple sub = new Tuple();
		sub.add(subject);
		
		if(object.startsWith("<"))
		{	sub.add(object);
			Tuple obj = new Tuple();
			obj.add(object);
			obj.add(subject);
			functionCall.getOutputCollector().add( obj );
		}
		else
			sub.add("-");
		functionCall.getOutputCollector().add( sub );
	}

	public SplitSubObj() {
		 super(1, new Fields("s","o"));
	}

}
