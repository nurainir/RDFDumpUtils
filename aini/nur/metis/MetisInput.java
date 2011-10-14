/**
 * aini.nur.metis-MetisInput.java
 * input of METIS in graph file
 * @author : Nur Aini Rakhmawati
 * @since  : 14 Oct 2011
 */

package aini.nur.metis;

import aini.nur.field.UniqueNumber;
import aini.nur.parser.N3parser;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.cogroup.LeftJoin;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class MetisInput {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Tap NtriplesInput =  new Lfs( new TextLine( new Fields( "offset", "line" ) ), args[ 0 ]);
		// parse input by tab (\t) character
		Pipe pipe= new Pipe("ntriple");
		pipe = new Each( pipe, new Fields( "line" ), new N3parser() );

		Pipe id = new Pipe("subject",pipe);
		id = new Each(id, new Fields("subject","object"), new SplitSubObj());
	    id = new GroupBy (id,new Fields("s"));
	    id = new Every(id, new UniqueNumber());
		
			
		Pipe list =  new Each(pipe, new Fields("subject","object"), new SplitSubObj());
		
		Pipe mergesubject =  new CoGroup( list, new Fields( "s" ), id, new Fields( "s" ), new Fields("s1","o","s2","id"), new LeftJoin());
		mergesubject = new Each( mergesubject, new Fields( "id","s1", "o"), new Identity() );
		
		Pipe mergeobject = new CoGroup( mergesubject, new Fields( "o" ), id, new Fields( "s" ), new Fields("id1","s1","o","s","id2"), new LeftJoin());
		
		
		Pipe neighbour = new Each( mergeobject, new Fields( "id1" ,"id2"), new Identity(new Fields( "ids" ,"ido")) );
		neighbour = new GroupBy (neighbour,new Fields("ids"),new Fields("ido"));
		neighbour =new Every(neighbour, new ObjAgg());
		neighbour =  new Each( neighbour, new Fields( "listObj"),new Identity());
		
		
		// store in the result file
		  Tap ResultS = new Lfs( new TextLine(), args[ 1 ]);
		final Flow SFlow = new FlowConnector().connect( NtriplesInput, ResultS,neighbour  );
			SFlow.start();
			SFlow.complete();
			
			
	        
	        
		
	        

	}

}
