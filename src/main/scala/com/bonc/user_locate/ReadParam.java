package com.bonc.user_locate;

import org.apache.spark.SparkConf;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;

public class ReadParam {
	
	public static void readXML(SparkConf config, String strPath) {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();  
        DocumentBuilder builder = null;
		try {
			builder = factory.newDocumentBuilder();
	        Document document = null;
			document = builder.parse(new File(strPath));
	        //get root element   
	        Element rootElement = document.getDocumentElement();   
	        //add root 
	        
	        NodeList tmpNodeList = rootElement.getChildNodes();
	        for (int i = 0; i < tmpNodeList.getLength(); i++) {
				Node node = tmpNodeList.item(i);
				//System.out.println(node.getNodeName() + " " + node.hasChildNodes() );
				if (node.hasChildNodes()) {
		       		NodeList sons = node.getChildNodes(); 
		       		if (sons.getLength()==1) {
		       			System.out.println(node.getNodeName() + "  " + node.getFirstChild().getNodeValue());
		       			config.set(node.getNodeName(), node.getFirstChild().getNodeValue());
		       		}else{
	        			//System.out.println(node.getNodeName() + "  " + node.getNodeType() + " " + node.getFirstChild().getNodeValue());
		    		 	for (int j=0; j < sons.getLength();j++)   
						{  
							Node jnode = sons.item(j);
							if ( jnode.getNodeType() == node.ELEMENT_NODE ) {
								System.out.println( node.getNodeName()  + "_" +jnode.getNodeName() +  "  " + jnode.getTextContent()) ;
								config.set( node.getNodeName()+"_"+jnode.getNodeName(), jnode.getTextContent());
							}
						}
					}
				}
			}
		} catch (ParserConfigurationException  | SAXException | IOException e) {
			System.out.println("Exception: " + e.getMessage());
		}  
    }

}
