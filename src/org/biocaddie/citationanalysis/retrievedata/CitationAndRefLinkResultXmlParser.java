package org.biocaddie.citationanalysis.retrievedata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.SAXException;
import org.xml.sax.Attributes;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

/**
 * RetrieveCitationFromEutils.java retrieves the citationLinks and citationReferences in XML format using eutils/elink service via HttpRequest.
 * If the XML file contains both citations and references, then this is the SAX XML Handler to parse that retrieved XML file. 
 */
public class CitationAndRefLinkResultXmlParser extends DefaultHandler{
    final static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    public Map<Integer, LinkSetShort> linkSetList = new HashMap<Integer, LinkSetShort>(); 
    
    //public LinkSetShort[] linkSetList = new LinkSetShort[11431198];
    int i =0;
    
	//public Map<LinkSetShort> linkSetList;
    String linkSetXmlFileName;
    String tmpValue;
    LinkSetShort linkSetTmp = new LinkSetShort();
    
    
    String previousTag="";
    String sep = " || ";
//    BufferedWriter out;
    
    public CitationAndRefLinkResultXmlParser(String linkSetXmlFileName) throws Exception {
        this.linkSetXmlFileName = linkSetXmlFileName;
        
       // linkSetList = new HashMap(<LinkSetShort>)(11431198);
        parseDocument();
    }

    private void parseDocument() throws Exception{
    	SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser parser = factory.newSAXParser();

		//System.out.println("Start converting XML file to txt file...");
		System.out.println("Start Time: " + dateFormat.format(new Date()));        
		/*String outFileName = linkSetXmlFileName.substring(0, linkSetXmlFileName.lastIndexOf(".")) + ".txt";
	    out = new BufferedWriter(new FileWriter(new File(outFileName)));
        out.write("* PubMedId || pubmed_pubmed_citedin || pubmed_pubmed_refs" ); 
        out.newLine();      
         */       
        parser.parse(linkSetXmlFileName, this);
        
    //    out.flush();       
    //    out.close();    	
        
		System.out.println("End Time  : " + dateFormat.format(new Date()));	    				
		System.out.println("DONE...");           
    }    

    @Override
    public void startElement(String s, String s1, String elementName, Attributes attributes) throws SAXException {
    	
        // new LinkSet
        if (elementName.equalsIgnoreCase("LinkSet")) {
        	linkSetTmp = new LinkSetShort();
        	//linkSetTmp.clear();
        }
        
        if (elementName.equalsIgnoreCase("IdList")) {
        	previousTag = "IdList";
        }
        
    }
    
    @Override
    public void endElement(String s, String s1, String element) throws SAXException {
    	// if end of linkSet element add to list
        if (element.equalsIgnoreCase("LinkSet")) {
        	linkSetList.put(linkSetTmp.id, linkSetTmp);
        	i++;
        	if ( ( i % 1000000 ) == 0 )
        		System.out.println(linkSetList.size() + " " + dateFormat.format(new Date()));
        	
	 /*       try {
				out.write(linkSetTmp.getId() + sep + linkSetTmp.citedinLinks.toString() + sep + linkSetTmp.refLinks.toString());
		        out.newLine();           		
	        } catch (IOException e) {
				e.printStackTrace();
				System.out.println("!!!ERROR Exiting...");
				System.exit(2);
			} 
       */ 	
        }

        if (element.equalsIgnoreCase("LinkName")) {        	
        	
        	if (tmpValue.equalsIgnoreCase("pubmed_pubmed_citedin"))
        		previousTag = "pubmed_pubmed_citedin";        	
        	
        	if (tmpValue.equalsIgnoreCase("pubmed_pubmed_refs"))
        		previousTag = "pubmed_pubmed_refs";        	        		
        }
        
        if (element.equalsIgnoreCase("id")) {
           	
        	if (previousTag.equals("IdList")){
        		linkSetTmp.id= Integer.valueOf(tmpValue);
        	}else if(previousTag.equals("pubmed_pubmed_citedin")){
        		Integer tmp = Integer.valueOf(tmpValue);
            	linkSetTmp.inLinks.put(tmp, tmp);
        		/*if (linkSetTmp.citedinLinks.length() > 0)
        			linkSetTmp.citedinLinks.append(",");
            	linkSetTmp.citedinLinks.append(tmpValue);*/
        	}else if(previousTag.equals("pubmed_pubmed_refs")){
        		Integer tmp = Integer.valueOf(tmpValue);
            	linkSetTmp.outLinks.put(tmp, tmp);
        		/*if (linkSetTmp.refLinks.length() > 0)
        			linkSetTmp.refLinks.append(",");
        		linkSetTmp.refLinks.append(tmpValue);*/
        	}else{ 
            	System.out.println("ERROR: Inconsistency in XML file");
            }
        }
    }
    
    @Override
    public void characters(char[] ac, int i, int j) throws SAXException {
    	tmpValue = new String(ac, i, j);
    }	
}

