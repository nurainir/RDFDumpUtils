package aini.nur.structure;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import aini.nur.parser.N3parser;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
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

public class RDFStructure {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Tap NtriplesInput =  new Lfs( new TextLine( new Fields( "offset", "line" ) ), args[ 0 ]);
		// parse input by n3parser
		// return  Fields( "subject" ,"predicate","object")
		Pipe pipe= new Pipe("ntriple");
	    pipe = new Each( pipe, new Fields( "line" ), new N3parser() );
	
	    // group by subject sorted subject
	    Pipe type = new Pipe("subject",pipe);
	    type = new GroupBy (type,new Fields("subject"),new Fields("subject","predicate"));
	    // return subject, type, predicateoftype
	    type = new Every(type, new Agg());
	    
	    // calculating coverage and weight coverage each type
	    Pipe coveragepipe = new GroupBy (type,new Fields("type"),new Fields("predicateoftype"));
	    coveragepipe = new Every(coveragepipe, new Coverage());
	    // only return type, coverage and weight 
	    coveragepipe =    new Each( coveragepipe, new Fields( "type", "coverage" ,"weightcoverage"), new Identity() );
	    
	    Tap Result = new Lfs( new TextLine(), args[ 1 ]+"/structure");
		  
			final Flow countFlow = new FlowConnector().connect( NtriplesInput, Result,coveragepipe  );
	        countFlow.start();
	        countFlow.complete();
	        
	        // opening the result then calculate coherence of dataset
	        long coverageSum = 0L;
	        try {
	        	BufferedWriter out = new BufferedWriter(new FileWriter(args[1]+"/coherence"));
	        	TupleEntryIterator iterator = countFlow.openSink();
	        	ArrayList<Integer> weight = new ArrayList<Integer>();
	        	ArrayList <Double> coverage = new ArrayList<Double>();
	    			while (iterator.hasNext())
	    			{
	    				TupleEntry t = iterator.next();
	    				String coulumn [] = t.getString("line").split("\t");
	    				coverageSum+=Long.parseLong(coulumn[2]);
	    				weight.add(Integer.parseInt(coulumn[2]));
	    				coverage.add(Double.parseDouble(coulumn[1]));
	    				    				
	    			}
	    			iterator.close();
	    			
	    			double coherence = 0;
	    			for(int i=0; i<weight.size();i++ )
	    			{
	    				double weightedCov = (double) weight.get(i) / coverageSum;
	    				coherence += weightedCov * coverage.get(i);
	    			}
	    			out.write(String.valueOf(coherence));
	    			out.close();
	    	} catch (IOException e) {
	    		e.printStackTrace();
	    	}		   
		


	}

}
