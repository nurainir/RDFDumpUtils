/**
 * aini.nur.metis-SubjObj.java
 * @author : Nur Aini Rakhmawati
 * @since  : 14 Oct 2011
 */
package aini.nur.metis;

import aini.nur.field.UniqueNumber;
import aini.nur.parser.N3parser;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;


public class SubjObj {

	public static void main(String[] args) {
		Tap NtriplesInput =  new Lfs( new TextLine( new Fields( "offset", "line" ) ), args[ 0 ]);
		Pipe pipe= new Pipe("ntriple");
		pipe = new Each( pipe, new Fields( "line" ), new N3parser() );
		//merge subject and object in URI
		pipe = new Each(pipe, new Fields("subject","object"), new SplitSubObj());
		
		  // id distinct subject-object
	    Pipe id = new Pipe("subject",pipe);
	    id = new GroupBy (id,new Fields("s"));
	    id = new Every(id, new UniqueNumber());
		
		 Tap Result = new Lfs( new TextLine(), args[ 1 ]);
			final Flow SFlow = new FlowConnector().connect( NtriplesInput, Result,id  );
			SFlow.start();
			SFlow.complete();
		
	}
	
}
