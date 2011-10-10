/**
 * Sorting Ntriples order by Subject, Predicate ascending
 * @author Nur Aini Rakhmawati
 * @since Oct 5, 2011
 * @return Fields("subject", "predicate", "object" ) 
 */

package aini.nur.sorting;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;

public class SortNTriples {

	/**
	 * @param args[0] input file / directory
	 * @param args[1] output file / directory
	 */
	public static void main(String[] args) {

		Tap NtriplesInput =  new Lfs( new TextLine( new Fields( "offset", "line" ) ), args[ 0 ]);
		Pipe pipe= new Pipe("ntriple");
		pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter(new Fields( "subject" ,"predicate","object"),"\t") );
		// sorting by subject then predicate
		pipe = new GroupBy (pipe,new Fields("subject", "predicate"),new Fields("subject", "predicate"));

		Tap NtriplesSortOutput = new Lfs( new TextLine(), args[ 1 ]);

	    final Flow sortFlow = new FlowConnector().connect( NtriplesInput, NtriplesSortOutput,pipe  );
	    sortFlow.start();
	    sortFlow.complete();


	}

}
