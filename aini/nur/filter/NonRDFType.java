/**
 * Removing triples containing RDFType 
 * @author Nur Aini Rakhmawati
 * @since Oct 12, 2011
 * @return Fields("subject", "predicate", "object" ) 
 */

package aini.nur.filter;

import aini.nur.parser.N3parser;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Filter;
import cascading.operation.filter.Not;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class NonRDFType {

	/**
	 * @param args[0] input file / directory
	 * @param args[1] output file / directory
	 */
	public static void main(String[] args) {
	
		Tap NtriplesInput =  new Lfs( new TextLine( new Fields( "offset", "line" ) ), args[ 0 ]);
		Pipe pipe= new Pipe("ntriple");
		pipe = new Each( pipe, new Fields( "line" ), new N3parser() );
		
		Filter rdftype = new RegexFilter("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
		Not notrdftype = new Not(rdftype);
	
		pipe = new Each (pipe,new Fields("predicate"),notrdftype);
	
		Tap NtriplesSortOutput = new Lfs( new TextLine(), args[ 1 ]);

	    final Flow filterFlow = new FlowConnector().connect( NtriplesInput, NtriplesSortOutput,pipe  );
	    filterFlow.start();
	    filterFlow.complete();


	}

}
