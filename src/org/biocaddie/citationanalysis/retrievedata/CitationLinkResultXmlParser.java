package org.biocaddie.citationanalysis.retrievedata;

import java.util.ArrayList;
import java.util.List;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.SAXException;
import org.xml.sax.Attributes;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

/**
 * RetrieveCitationFromEutils.java retrieves the citationLinks in XML format using eutils/elink service via HttpRequest. 
 * This is the SAX XML Handler to parse that retrieved XML file.
 */
public class CitationLinkResultXmlParser extends DefaultHandler{

    public List<LinkSet> linkSetList;
    String xmlFileName;
    String tmpValue;
    LinkSet linkSetTmp;
    String previousTag="";
    
    public CitationLinkResultXmlParser(String linkSetXmlFileName) throws Exception {
        this.xmlFileName = linkSetXmlFileName;
        linkSetList = new ArrayList<LinkSet>();
        parseDocument();
    }

    private void parseDocument() throws Exception{
    	SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser parser = factory.newSAXParser();
        parser.parse(xmlFileName, this);
    }
    
    @Override
    public void startElement(String s, String s1, String elementName, Attributes attributes) throws SAXException {
    	
        // new LinkSet
        if (elementName.equalsIgnoreCase("LinkSet")) {
        	linkSetTmp = new LinkSet();
        }
        
        if (elementName.equalsIgnoreCase("IdList")) {
        	previousTag = "IdList";
        }
        
        if (elementName.equalsIgnoreCase("Link")) {
        	previousTag = "Link";
        }        
    }
    
    @Override
    public void endElement(String s, String s1, String element) throws SAXException {
    	// if end of linkSet element add to list
        if (element.equals("LinkSet")) {
        	linkSetList.add(linkSetTmp);
        }
            
        if (element.equalsIgnoreCase("id")) {
           	
        	if (previousTag.equals("IdList"))
        		linkSetTmp.setId(tmpValue);
            else if(previousTag.equals("Link"))
            	linkSetTmp.getCitedinLinkIds().add(tmpValue);
            else 
            	System.out.println("ERROR: Inconsistency in XML file");
            }
    }
    
    @Override
    public void characters(char[] ac, int i, int j) throws SAXException {
    	tmpValue = new String(ac, i, j);
    }
}


