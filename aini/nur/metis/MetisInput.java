/**
 * aini.nur.metis-MetisInput.java
 * input of METIS in graph file
 * @author : Nur Aini Rakhmawati
 * @since  : 14 Oct 2011
 */

package aini.nur.metis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import aini.nur.field.UniqueNumber;
import aini.nur.parser.N3parser;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Filter;
import cascading.operation.Identity;
import cascading.operation.aggregator.First;
import cascading.operation.filter.Not;
import cascading.operation.regex.RegexFilter;
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
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class MetisInput {
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Tap NtriplesInput =  new Lfs( new TextLine( new Fields( "offset", "line" ) ), args[ 0 ]);
		// parsing ntriples
		Pipe pipe= new Pipe("ntriple");
		pipe = new Each( pipe, new Fields( "line" ), new N3parser() );
		
		/*
		 * created
		 * S - 0
		 * O - S
		 */
		Pipe list = new Pipe("list",pipe);
		list  =new Each(list, new Fields("subject","object"), new SplitSubObj());
		// giving ID for each unique S and O
	    Pipe id = new Pipe("id",list);
	    id= new GroupBy (id,new Fields("s"));
	    id = new Every(id, new UniqueNumber());
	    id= new Each(id,new Fields( "id","s"),new Identity(new Fields("id1","s1")));
		// map id for each subject		
		Pipe mergesubject =  new CoGroup( list, new Fields( "s" ), id, new Fields( "s1" ), new LeftJoin());
		mergesubject = new Each( mergesubject, new Fields( "id1","s", "o"), new Identity(new Fields("ids","ss","os")) );
		// map id for each object
		Pipe mergeobject = new CoGroup( mergesubject, new Fields( "os" ), id, new Fields( "s1" ), new LeftJoin());
		
		// S each O
		Pipe neighbour = new Each( mergeobject, new Fields( "ids" ,"id1"), new Identity(new Fields( "ids" ,"ido")) );
		neighbour = new GroupBy (neighbour,new Fields("ids"),new Fields("ido"));
		// aggregate all Objects
		neighbour =new Every(neighbour, new ObjAgg());
		neighbour =  new Each( neighbour, new Fields( "listObj"),new Identity());
		
		// store in the result file
		  Tap Result = new Lfs( new TextLine(), args[ 1 ]);
		final Flow flow = new FlowConnector().connect( NtriplesInput, Result,neighbour  );
		flow.start();
		flow.complete();
		
		BufferedWriter out = new BufferedWriter(new FileWriter(args[1]+".graph"));
		int vertex =0;
		TupleEntryIterator iterator = flow.openSink();
		StringBuilder sb = new StringBuilder();
		
		while (iterator.hasNext())
		{
			vertex++;
			TupleEntry t  = iterator.next();
			String line = t.getString("line");
			if(line!=null)
			sb.append(line+'\n');
		}
		iterator.close();
	    
		// Calculating edge
		Tap edgeTap = new Lfs( new TextLine(), args[ 1 ]+"/e");
		Filter literal = new RegexFilter("^\"");
		Not notliteral = new Not(literal);
		
		Pipe epipe = new Each (pipe,new Fields("object"),notliteral);
		epipe = new Each (epipe, new Fields("subject","object"),new concateSubObj());
		epipe = new GroupBy( epipe, new Fields("concate") );
		epipe = new Every( epipe, new Fields("concate"), new First(), Fields.RESULTS );
		final Flow eflow = new FlowConnector().connect( NtriplesInput, edgeTap,epipe  );
		eflow.start();
		eflow.complete();
	        

		int edge =0;
		iterator = eflow.openSink();
		while (iterator.hasNext())
		{
			edge++;
			iterator.next();
		}
		iterator.close();
		out.write(vertex+" "+edge+"\n");
		out.write(sb.toString());
		out.close();
	}

}
