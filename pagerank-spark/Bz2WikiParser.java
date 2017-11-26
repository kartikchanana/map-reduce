import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Decompresses bz2 file and parses Wikipages on each line. */
public class Bz2WikiParser {
    private static Pattern namePattern;
    private static Pattern linkPattern;
    static {
        //Aly_&_AJ_0e76:<!DOCTYPE html PUBLIC
        //& Fire</a></li><li><a hre (illegal content here)
        // Keep only html pages not containing tilde (~). and containing only ([&][^&#][^&reg][^&amp][^&lt][^&gt][^&quot][^&apos])\w
        namePattern = Pattern.compile("^([^~]+)$");
        // Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
    }


    public static String adjacencyList(String entry) {
        String output = "";
        try {
            // Configure parser.
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = spf.newSAXParser();
            XMLReader xmlReader = saxParser.getXMLReader();
            // Parser fills this list with linked page names.
            List<String> linkPageNames = new LinkedList<String>();
            xmlReader.setContentHandler(new WikiParser(linkPageNames));

            String line=entry;
            if (line != null) {
                // Each line formatted as (Wiki-page-name:Wiki-page-html).
                int delimLoc = line.indexOf(':');
                String pageName = line.substring(0, delimLoc);
                String html = line.substring(delimLoc + 1);
                Matcher matcher = namePattern.matcher(pageName);

                if(matcher.find()){
                    output += pageName;
                    // Parse page and fill list of linked pages.
                    linkPageNames.clear();
                    try {
                        xmlReader.parse(new InputSource(new StringReader(html.replace(" & ", " &amp; "))));
                        String links = linkPageNames.toString();
                        output += "=>" + links.substring(1, links.length()-1);
                    } catch (Exception e) {
                        return "invalid";
                        // Discard ill-formatted pages.
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    /** Parses a Wikipage, finding links inside bodyContent div element. */
    private static class WikiParser extends DefaultHandler {
        /** List of linked pages; filled by parser. */
        private List<String> linkPageNames;
        /** Nesting depth inside bodyContent div element. */
        private int count = 0;

        public WikiParser(List<String> linkPageNames) {
            super();
            this.linkPageNames = linkPageNames;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            super.startElement(uri, localName, qName, attributes);
            if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
                // Beginning of bodyContent div element.
                count = 1;
            } else if (count > 0 && "a".equalsIgnoreCase(qName)) {
                // Anchor tag inside bodyContent div element.
                count++;
                String link = attributes.getValue("href");
                if (link == null) {
                    return;
                }
                try {
                    // Decode escaped characters in URL.
                    link = URLDecoder.decode(link, "UTF-8");
                } catch (Exception e) {
                    // Wiki-weirdness; use link as is.
                }
                // Keep only html filenames ending relative paths and not containing tilde (~).
                Matcher matcher = linkPattern.matcher(link);
                if (matcher.find()) {
                    linkPageNames.add(matcher.group(1));
                }
            } else if (count > 0) {
                // Other element inside bodyContent div.
                count++;
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);
            if (count > 0) {
                // End of element inside bodyContent div.
                count--;
            }
        }
    }
}