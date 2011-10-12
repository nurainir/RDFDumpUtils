/**
 * Parsing Ntriples from textline order by Subject, Predicate ascending
 * @author Nur Aini Rakhmawati
 * @since Oct 10, 2011
 * @return Fields("subject", "predicate", "object" ) 
 */

package aini.nur.parser;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;



public class N3parser extends BaseOperation implements Function
{
	
	  public N3parser() {
		    // expects 1 arguments, fail otherwise
		    super(1, new Fields("subject", "predicate", "object"));
		  }

	@Override
	public void operate(FlowProcess arg0, FunctionCall functionCall) {
		TupleEntry arguments = functionCall.getArguments();
		// create a Tuple to hold our result values
		Tuple result = new Tuple();
		 String n3 = ((String) arguments.get(0)).replace('\t', ' ');
		 
		    Node[] nodes = null;
		    try {
		      nodes = NxParser.parseNodes(n3);
		    } catch (final Exception e) {
		     
		      return;
		    }

		   
		    if (nodes != null && nodes.length == 3) {
		      result.addAll(nodes[0].toN3(),nodes[1].toN3(),nodes[2].toN3());
		
		functionCall.getOutputCollector().add( result );

		
	}

}}
