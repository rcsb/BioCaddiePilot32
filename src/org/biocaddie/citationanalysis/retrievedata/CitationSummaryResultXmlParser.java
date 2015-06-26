package org.biocaddie.citationanalysis.retrievedata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.SAXException;
import org.xml.sax.Attributes;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

/**
 * RetrieveSummaryFromEutils.java retrieves the summary of PubMed id's in XML format using eutils/esummary service via HttpRequest.
 * This is the SAX XML Handler to parse that retrieved XML file.
 * Summary XML file is very big such as ~25GB, because of that we read the XML file and write the attributes we needed to a text file.
 * PubMedId || Title || PubYear || JournalName || JournalId || LastAuthor  
 */
public class CitationSummaryResultXmlParser extends DefaultHandler{
    final static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    public List<DocSum> docSumList;
    String docSumXmlFileName;
    StringBuilder tmpValue = new StringBuilder();
    DocSum docSumTmp = new DocSum();	
    String currentItemAttributeValue;
    String sep = " || ";
    BufferedWriter out;
	
    public CitationSummaryResultXmlParser(String docSumXmlFileName) throws Exception {
        this.docSumXmlFileName = docSumXmlFileName;
        //docSumList = new ArrayList<DocSum>();
        parseDocument();
    }

    private void parseDocument() throws Exception{
    	SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser parser = factory.newSAXParser();

		System.out.println("Start converting XML file to txt file...");
		System.out.println("Start Time: " + dateFormat.format(new Date()));        
		String outFileName = docSumXmlFileName.substring(0, docSumXmlFileName.lastIndexOf(".")) + ".txt";
	    out = new BufferedWriter(new FileWriter(new File(outFileName)));
        out.write("* PubMedId || Title || PubYear || JournalName || JournalId || LastAuthor" ); 
        out.newLine();      
        		
        parser.parse(docSumXmlFileName, this);

        out.flush();       
        out.close();    		 
		System.out.println("End Time  : " + dateFormat.format(new Date()));	    				
		System.out.println("DONE...");        
    }

    @Override
    public void startElement(String s, String s1, String elementName, Attributes attributes) throws SAXException {
        // new DocSum
        if (elementName.equalsIgnoreCase("DocSum")) {
        	//docSumTmp = new DocSum();
        	docSumTmp.clear();
        	currentItemAttributeValue="";
        	tmpValue.setLength(0);
        }   
        
        if (elementName.equalsIgnoreCase("Item")) {
        	currentItemAttributeValue = attributes.getValue("Name");
        	tmpValue.setLength(0);
        }
        
    }
    
    @Override
    public void endElement(String s, String s1, String element) throws SAXException {    	
    	// if end of docSum element add to list
    	if (element.equals("DocSum")) {
    		//docSumList.add(docSumTmp);
	        try {
				out.write(docSumTmp.getId() + sep + docSumTmp.getTitle() + sep + docSumTmp.getPubDate().substring(0,4) + sep + docSumTmp.getFullJournalName() + sep + docSumTmp.getNlmUniqueID() + sep + docSumTmp.getLastAuthor());
		        out.newLine();           		
	        } catch (IOException e) {
				e.printStackTrace();
				System.out.println("!!!ERROR Exiting...");
				System.exit(2);
			} 
    	}            
        if (element.equalsIgnoreCase("Id")) {
    		docSumTmp.setId(tmpValue.toString());
        }
        if (element.equalsIgnoreCase("Item")) {        	
        	if (currentItemAttributeValue.equalsIgnoreCase("Title") )
        		docSumTmp.setTitle(tmpValue.toString());
        	if (currentItemAttributeValue.equalsIgnoreCase("LastAuthor") )
        		docSumTmp.setLastAuthor(tmpValue.toString());
        	if (currentItemAttributeValue.equalsIgnoreCase("PubDate") )
        		docSumTmp.setPubDate(tmpValue.toString());
        	if (currentItemAttributeValue.equalsIgnoreCase("NlmUniqueID") )
        		docSumTmp.setNlmUniqueID(tmpValue.toString());
        	if (currentItemAttributeValue.equalsIgnoreCase("FullJournalName") )
        		docSumTmp.setFullJournalName(tmpValue.toString());        	
        }    	
    	
    }
    
    @Override
    public void characters(char[] ac, int i, int j) throws SAXException {
    	tmpValue.append(ac, i, j);
    }	
}

