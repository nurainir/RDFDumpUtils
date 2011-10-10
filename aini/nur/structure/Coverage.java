/**
 * Calculating coverage and weight of each type 
 * @author Nur Aini Rakhmawati
 * @since Oct 10, 2011
 * @return  Fields("coverage","weightcoverage") 
 */

package aini.nur.structure;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class Coverage extends BaseOperation implements Buffer
{

	
	public Coverage() {
		super(1, new Fields("coverage","weightcoverage"));
		}

	@Override
	public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
	
		// group by type
		Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
		long predCountSum = 0;
		int distPredicate=0;
		int distInstance=0;
		String lastpredicate ="-";
		String lastinstance ="-";
		Set<String> instances = new HashSet<String>();
		while( arguments.hasNext() )
		{
			
			TupleEntry cur =arguments.next();
			String ptype = cur.getString("predicateoftype");
			String instance = cur.getString("subject");
					
			if(!ptype.equals(lastpredicate) && !ptype.equals("-"))
				distPredicate++;
				
			instances.add(instance);
			
			if (!ptype.equals("-"))
			{
			if(!ptype.equals(lastpredicate)  || instance.equals(lastinstance) )
				predCountSum++;
			else if(ptype.equals(lastpredicate) || !instance.equals(lastinstance) )
				predCountSum++;
			else if(!ptype.equals(lastpredicate) && !instance.equals(lastinstance) )
				predCountSum++;
			}

			lastpredicate = ptype;
			lastinstance = instance;
		}

		distInstance=instances.size();
	
		Tuple result = new Tuple();
		//type coverage
		if(predCountSum>0)
		result.add((double) predCountSum / (distPredicate * distInstance));
		else
			result.add(0);
		// weight coverage
		result.add(distPredicate + distInstance);
		
		bufferCall.getOutputCollector().add( result );

	}

}
