/**
 * Counting number of distinct subject and object as well as triples of each properties
 * @author Nur Aini Rakhmawati
 * @since Oct 5, 2008
 * @return Fields( "predicate1", "propsub" ,"propob","triples") = predicate, distinct subject, distinct object, number of triples  
 */
package aini.nur.statistic;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import aini.nur.parser.N3parser;

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
		
		Tap NtriplesInput =  new Lfs( new TextLine( new Fields( "offset", "line" ) ), args[ 0 ]);
		// parse input by tab (\t) character
		Pipe pipe= new Pipe("ntriple");
		pipe = new Each( pipe, new Fields( "line" ), new N3parser() );

	    // count distinct subject
	    Pipe subjectpipe = new Pipe("subject",pipe);
	    subjectpipe = new GroupBy (subjectpipe,new Fields("predicate"),new Fields("subject"));
		subjectpipe = new Every(subjectpipe, new PropAgg());
		
			  // store in the result file
		  Tap Result = new Lfs( new TextLine(), args[ 1 ]+"/prop");
		  
			final Flow countFlow = new FlowConnector().connect( NtriplesInput, Result,subjectpipe  );
	        countFlow.start();
	        countFlow.complete();
		
	        int i = 0;
	        long ntriples =0, blanknode =0, uri =0, literal =0;
	        
	        try {
	        	BufferedWriter out = new BufferedWriter(new FileWriter(args[1]+"/stat"));
	        	TupleEntryIterator iterator = countFlow.openSink();
	        	double avgout =0, avgin =0;
	        	int entity=0;
	        	
	    			while (iterator.hasNext())
	    			{
	    				i++;
	    				TupleEntry t = iterator.next();
	    				String coulumn [] = t.getString("line").split("\t");
	    				if(coulumn[0].equalsIgnoreCase("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"))
	    					entity=Integer.parseInt(coulumn[3]);
	    			
	    				avgout+=Long.parseLong(coulumn[1]);
	    				avgin+=Long.parseLong(coulumn[2]);
	    				ntriples+=Long.parseLong(coulumn[3]);
	    				blanknode+=Long.parseLong(coulumn[4]);
	    				literal+=Long.parseLong(coulumn[5]);
	    				uri+=Long.parseLong(coulumn[6]);
	    			}
	    			iterator.close();
	    			 out.write("AvgIn :"+avgin/i+"\n");
	    			 out.write("AvgOut :"+avgout/i+"\n");
	    			 out.write("ntriples :"+ntriples+"\n");
	    			 out.write("blanknode :"+blanknode+"\n");
	    			 out.write("literal :"+literal+"\n");
	    			 out.write("uri :"+uri+"\n");
	    			 out.write("entity :"+entity+"\n");
	    			 
	    			 out.close();
	    	} catch (IOException e) {
	    		// TODO Auto-generated catch block
	    		e.printStackTrace();
	    	}		   
	        
	}

}
