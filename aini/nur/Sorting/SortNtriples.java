package aini.nur;

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

public class SortNtriples {

	/**
	 * @param args[0] input file / directory
	 * @param args[1] output file / directory
	 */
	public static void main(String[] args) {

	Tap NtriplesInput =  new Lfs( new TextLine( new Fields( "offset", "line" ) ), args[ 0 ]);
	Tap NtriplesSortOutput = new Lfs( new TextLine(), args[ 1 ]);
	
	Pipe pipe= new Pipe("ntriple");
	pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter(new Fields( "subject" ,"predicate","object")," ") );
	// this class sorts RDF by its subject then predicate
	pipe = new GroupBy (pipe,new Fields("subject", "predicate"));
	
	final Flow countFlow = new FlowConnector().connect( NtriplesInput, NtriplesSortOutput,pipe  );
        countFlow.start();
        countFlow.complete();
	}
}
