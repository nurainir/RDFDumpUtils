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
import cascading.operation.Filter;
import cascading.operation.aggregator.First;
import cascading.operation.filter.Not;
import cascading.operation.regex.RegexFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
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
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		Tap NtriplesInput =  new Lfs( new TextLine( new Fields( "offset", "line" ) ), args[ 0 ]);
		// parse Ntriple
		Pipe pipe= new Pipe("ntriple");
		pipe = new Each( pipe, new Fields( "line" ), new N3parser() );

	    // count distinct subject and object each properties
	    Pipe proppipe = new Pipe("prop",pipe);
	    proppipe = new GroupBy (proppipe,new Fields("predicate"),new Fields("subject"));
	    proppipe = new Every(proppipe, new PropAgg());
		
	    // count distinct subject all dataset
	    Pipe subpipe = new Pipe("subject",pipe);
	    subpipe = new GroupBy (subpipe,new Fields("subject"));
	    subpipe = new Every( subpipe, new Fields("subject"), new First(), Fields.RESULTS );
	    
	    // count distinct object all dataset
	    Pipe objpipe = new Pipe("object",pipe);
	    objpipe = new GroupBy (objpipe,new Fields("object"));
	    objpipe = new Every( objpipe, new Fields("object"), new First(), Fields.RESULTS );
	    
	    //count Class
	    // remove not RDFtype
		Filter rdftype = new RegexFilter("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
		//Not notrdftype = new Not(rdftype);
	    //select distinct
	    Pipe classpipe = new Pipe("class",pipe);
	    classpipe = new Each (classpipe,new Fields("predicate"),rdftype);
	    classpipe = new GroupBy (classpipe,new Fields("object"));
	    classpipe = new Every( classpipe, new Fields("object"), new First(), Fields.RESULTS );
	    
	    
	    // store properties
		Tap prop = new Lfs( new TextLine(), args[ 1 ]+"/prop");
		
		final Flow PFlow = new FlowConnector().connect( NtriplesInput, prop,proppipe  );
	    PFlow.start();
	    PFlow.complete();
		
	    //store subjects
	    Tap sub = new Lfs( new TextLine(), args[ 1 ]+"/sub");
	    final Flow SFlow = new FlowConnector().connect( NtriplesInput, sub,subpipe  );
	    SFlow.start();
	    SFlow.complete();
	    
	    //store objects
	    Tap obj = new Lfs( new TextLine(), args[ 1 ]+"/obj");
	    final Flow OFlow = new FlowConnector().connect( NtriplesInput, obj,objpipe  );
	    OFlow.start();
	    OFlow.complete();
	    
	    //store class
	    Tap clas = new Lfs( new TextLine(), args[ 1 ]+"/class");
	    final Flow CFlow = new FlowConnector().connect( NtriplesInput, clas,classpipe  );
	    CFlow.start();
	    CFlow.complete();
	    
	    int i = 0;
	    long ntriples =0, blanknode =0, uri =0, literal =0,subject =0, object =0,classes =0;
	    int entity=0;  
	       
	        	BufferedWriter out = new BufferedWriter(new FileWriter(args[1]+"/stat"));
	        	TupleEntryIterator iterator = PFlow.openSink();
	        	double avgout =0, avgin =0;
	        	
	        	
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
	    			
	    			 iterator = SFlow.openSink();
	    			 while (iterator.hasNext())
		    			{
	    				 subject++;
	    				 iterator.next();
		    			}
	    			 
	    			 iterator.close();
	    			 
	    			 iterator = OFlow.openSink();
	    			 while (iterator.hasNext())
		    			{
	    				 object++;
	    				 iterator.next();
		    			}
	    			 iterator.close();
	    			 
	    			 iterator = CFlow.openSink();
	    			 while (iterator.hasNext())
		    			{
	    				 classes++;
	    				 iterator.next();
		    			}
	    			 iterator.close();
	    			 
	    			 out.write("subject :"+subject+"\n");
	    			 out.write("predicate :"+object+"\n");
	    			 out.write("class :"+classes+"\n");
	    			 out.close();
	    			   
	        
	}

}
