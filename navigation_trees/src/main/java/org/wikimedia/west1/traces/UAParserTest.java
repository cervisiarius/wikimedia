package org.wikimedia.west1.traces;

import java.io.IOException;

import ua_parser.Parser;

public class UAParserTest {

  public static void main(String[] args) throws IOException {
    Parser uaParser = new Parser();
    System.out.println(uaParser.parse("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"));
    System.out.println(uaParser.parse("Googlebot/2.1 (+http://www.google.com/bot.html)"));
    System.out.println(uaParser.parse("Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)"));
    System.out.println(uaParser.parse("Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)"));
    System.out.println(uaParser.parse("AppEngine-Google; (+http://code.google.com/appengine; appid: webponline5)"));
    System.out.println(uaParser.parse("Mozilla/5.0 (compatible; MSIE or Firefox mutant; not on Windows server; + http://tab.search.daum.net/aboutWebSearch.html) Daumoa/3.0"));
    System.out.println(uaParser.parse("AppleDictionaryService/184.4"));
    System.out.println(uaParser.parse("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/43.0.2357.81 Chrome/43.0.2357.81 Safari/537.36"));
    System.out.println(uaParser.parse("Mozilla/5.0 (Windows NT 6.0; rv:2.0) Gecko/20100101 Firefox/4.0 Opera 12.14"));
    System.out.println(uaParser.parse("Galaxy/1.0 [en] (Mac OS X 10.5.6; U; en)"));
    System.out.println(uaParser.parse("Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 521)"));
    
}

}
