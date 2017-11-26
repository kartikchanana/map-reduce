import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class parser {

  static class Map extends Mapper<Object, Text, Text, Text> {

        // Keep only html pages not containing tilde (~). and containing only ([&][^&#][^&reg][^&amp][^&lt][^&gt][^&quot][^&apos])\w
        Pattern namePattern = Pattern.compile("^([^~]+)$");

        List<String> linkPageNames;
        XMLReader xmlReader = null;

        @Override
        public void setup(Context context) {
            // Configure parser.
            SAXParserFactory spf = SAXParserFactory.newInstance();
            try {
                spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (SAXNotSupportedException e) {
                e.printStackTrace();
            } catch (SAXNotRecognizedException e) {
                e.printStackTrace();
            }
            SAXParser saxParser = null;
            try {
                saxParser = spf.newSAXParser();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            }

            try {
                xmlReader = saxParser.getXMLReader();
            } catch (SAXException e) {
                e.printStackTrace();
            }
            // Parser fills this list with linked page names.
            linkPageNames = new LinkedList<String>();
            xmlReader.setContentHandler(new WikiParser(linkPageNames));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Each line formatted as (Wiki-page-name:Wiki-page-html).
            String line = value.toString();
            int delimLoc = line.indexOf(':');
            String pageName = line.substring(0, delimLoc);
            String html = line.substring(delimLoc + 1);
            Matcher matcher = namePattern.matcher(pageName);
            if (matcher.find()) {
                Text output = new Text();
                // Parse page and fill list of linked pages.
                linkPageNames.clear();
                try {
                    xmlReader.parse(new InputSource(new StringReader(html.replace(" & ", " &amp; "))));
                    output.set(linkPageNames.toString());
                    context.getCounter(nodesCounter.nodesCount).increment(1L);
                } catch (Exception e) {
                    System.out.println(e);
                    // Discard ill-formatted pages.}
                }
                context.write(new Text(pageName), output);
            } else {
                return;
            }
        }
  }

    /** Parses a Wikipage, finding links inside bodyContent div element. */
    private static class WikiParser extends DefaultHandler {
        /** List of linked pages; filled by parser. */
        private List<String> linkPageNames;
        /** Nesting depth inside bodyContent div element. */
        private int count = 0;

        // Keep only html filenames ending relative paths and not containing tilde (~).
        Pattern linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
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

