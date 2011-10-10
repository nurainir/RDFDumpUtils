/**
 * Counting number of distinct subject and object as well as triples of each properties
 * @author Nur Aini Rakhmawati
 * @since Oct 5, 2011
 * @return Fields( "predicate1", "propsub" ,"propob","triples") = predicate, distinct subject, distinct object, number of triples  
 */
package aini.nur.statistic;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.cogroup.InnerJoin;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class RDFPropStat {

	/**
	 * @param args[0] input file / directory
	 * @param args[1] output file / directory
	 */
	public static void main(String[] args) {
		Fields ntriplefield = new Fields( "subject" ,"predicate","object");
		Tap NtriplesInput =  new Lfs( new TextLine( new Fields( "offset", "line" ) ), args[ 0 ]);
		// parse input by tab (\t) character
		Pipe pipe= new Pipe("ntriple");
		pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter(ntriplefield,"\t") );

	    // count distinct subject
	    Pipe subjectpipe = new Pipe("subject",pipe);
	    subjectpipe = new GroupBy (subjectpipe,new Fields("predicate"),new Fields("subject"));
		subjectpipe = new Every(subjectpipe, new SubAgg());
		
		// count distinct object
	    Pipe objectpipe = new Pipe("object",pipe);
	    objectpipe = new GroupBy (objectpipe,new Fields("predicate"),new Fields("object"));
		objectpipe = new Every(objectpipe, new ObjAgg());
	    
		// join result from subject and object
		  Pipe cross = new CoGroup( subjectpipe, new Fields( "predicate" ), objectpipe, new Fields( "predicate" ), new Fields( "predicate1","propsub", "triples","predicate2", "propob" ),new InnerJoin() );
		  cross =    new Each( cross, new Fields( "predicate1", "propsub" ,"propob","triples"), new Identity() );
		
		  
		  // store in the result file
		  Tap Result = new Lfs( new TextLine(), args[ 1 ]);
		  
			final Flow countFlow = new FlowConnector().connect( NtriplesInput, Result,cross  );
	        countFlow.start();
	        countFlow.complete();
		
	        long i = 0L;
	        try {
	        	BufferedWriter out = new BufferedWriter(new FileWriter("average"));
	        	TupleEntryIterator iterator = countFlow.openSink();
	        	double avgout =0, avgin =0;
	    			while (iterator.hasNext())
	    			{
	    				i++;
	    				TupleEntry t = iterator.next();
	    				String coulumn [] = t.getString("line").split("\t");
	    				avgout+=Long.parseLong(coulumn[1]);
	    				avgin+=Long.parseLong(coulumn[2]);
	    			}
	    			iterator.close();
	    			 out.write("AvgIn :"+avgin/i+"\n");
	    			 out.write("AvgOut :"+avgout/i+"\n");
	    			 out.close();
	    	} catch (IOException e) {
	    		// TODO Auto-generated catch block
	    		e.printStackTrace();
	    	}		   
	        
	}

}
