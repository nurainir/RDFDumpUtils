package aini.nur.metis;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class concateSubObj extends BaseOperation implements Function {

	@Override
	public void operate(FlowProcess arg0, FunctionCall functionCall) {
		TupleEntry arguments = functionCall.getArguments();
		String subject = arguments.getString("subject");
		String object = arguments.getString("object");
		Tuple concate = new Tuple();
		concate.add(subject+object);
		functionCall.getOutputCollector().add( concate );
		
	}

	public concateSubObj() {
		 super(1, new Fields("concate"));
	}

}
