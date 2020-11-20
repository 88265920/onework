package com.onework.core.utils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author kangj
 * @date 2020/11/20
 **/
public class HadoopXmlUtils {

    public static Map<String, String> readXML(String xmlPath) throws DocumentException {
        SAXReader reader = new SAXReader();
        Document document = reader.read(new File(xmlPath));
        Element root = document.getRootElement();

        Map<String, String> values = new HashMap<>();

        Iterator<Element> iterator = root.elementIterator();
        while (iterator.hasNext()) {
            Element node = iterator.next();
            String name = node.getName().trim().toLowerCase();
            if (name.equals("property")) {
                values.put(node.elementTextTrim("name"), node.elementTextTrim("value"));
            }
        }

        return values;
    }
}
