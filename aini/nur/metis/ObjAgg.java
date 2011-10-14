/**
 * aini.nur.metis-ObjAgg.java
 * List of object owned by subject
 * @author : Nur Aini Rakhmawati
 * @since  : 14 Oct 2011
 */

package aini.nur.metis;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ObjAgg extends BaseOperation implements Buffer {
	
	public ObjAgg() {
		super(1, new Fields("listObj","numbObj"));
	}

	@Override
	public void operate(FlowProcess arg0, BufferCall bufferCall) {
		Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
		StringBuilder sb = new StringBuilder();
		int numbObj =0;
		while( arguments.hasNext() )
		{
			TupleEntry cur =arguments.next();
			String obj = cur.getString("ido");
			if(obj != null)
			{
			if(arguments.hasNext())
				sb.append(obj+' ');
			else
				sb.append(obj);
			numbObj++;
			}
		}
		
		Tuple result = new Tuple();
		result.add(sb.toString());
		result.add(numbObj);
		bufferCall.getOutputCollector().add( result );
		
		}
		
	}


