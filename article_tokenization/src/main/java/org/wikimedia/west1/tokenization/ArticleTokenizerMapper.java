package org.wikimedia.west1.tokenization;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.process.PTBTokenizer.PTBTokenizerFactory;
import edu.stanford.nlp.process.Tokenizer;
import edu.stanford.nlp.process.WordTokenFactory;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.wikipedia.miner.util.MarkupStripper;

public class ArticleTokenizerMapper implements Mapper<Text, Text, Text, Text> {

	private static Pattern NAMESPACE_PATTERN = Pattern.compile("(?s).*<ns>0</ns>.*");
	private static Pattern REDIRECT_PATTERN = Pattern.compile("(?s).*<redirect title=\"(.*)");
	private static Pattern TITLE_PATTERN = Pattern.compile("(?s).*?<title>(.*?)</title>.*");
	private static Pattern CONTENT_PATTERN = Pattern
	    .compile("(?s).*?<text xml:space=\"preserve\">(.*)</text>.*");

	private PTBTokenizerFactory<Word> tokenizerFactory;

	private MarkupStripper stripper = new MarkupStripper();

	private static enum HADOOP_COUNTERS {
		NON_MAIN_NAMESPACE, NO_TITLE, REDIRECT, OK_ARTICLE, MAP_EXCEPTION
	}

	public String format(String markup) {
		// this is a hacky way of removing emphasis (as EmphasisResolver seems to be buggy; since it's
		// also used by stripAllButInternalLinksAndEmphasis, we need to remove emphasis manually first)
		markup = markup.replaceAll("'{6}", "'");
		markup = markup.replaceAll("'{5}", "");
		markup = markup.replaceAll("'{4}", "'");
		markup = markup.replaceAll("'{3}", "");
		markup = markup.replaceAll("'{2}", "");
		markup = stripper.stripAllButInternalLinksAndEmphasis(markup, null);
		markup = stripper.stripInternalLinks(markup, null);
		markup = stripper.stripExcessNewlines(markup);
		markup = StringEscapeUtils.unescapeHtml(markup);
		markup = markup.replace('\u2019', '\'');
		return markup;
	}

	public String tokenize(String str) {
		Tokenizer<Word> tokenizer = tokenizerFactory.getTokenizer(new StringReader(str));
		StringBuffer buf = new StringBuffer();
		String sep = "";
		while (tokenizer.hasNext()) {
			buf.append(sep).append(tokenizer.next().word());
			sep = " ";
		}
		return buf.toString();
	}

	public String processWikiText(String s) {
		return tokenize(format(s).replaceAll("\\s*\n+\\s*", "\n"));
	}

	// @Override
	public void configure(JobConf conf) {
		tokenizerFactory = PTBTokenizerFactory.newPTBTokenizerFactory(new WordTokenFactory(),
		    "tokenizeNLs=true");
	}

	// @Override
	public void close() throws IOException {
	}

	// @Override
	public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
	    throws IOException {
		try {
			String xml = key.toString();
			String title, content;
			// We're only interested in the main namespace.
			if (!NAMESPACE_PATTERN.matcher(xml).matches()) {
				reporter.incrCounter(HADOOP_COUNTERS.NON_MAIN_NAMESPACE, 1);
				return;
			}
			Matcher m = TITLE_PATTERN.matcher(xml);
			if (m.matches()) {
				title = StringEscapeUtils.unescapeHtml(m.group(1));
				m = REDIRECT_PATTERN.matcher(xml);
				if (m.matches()) {
					String redirectTarget = m.group(1);
					reporter.incrCounter(HADOOP_COUNTERS.REDIRECT, 1);
					output.collect(new Text(title), new Text(String.format("%s\t%s", redirectTarget, "")));
				} else {
					m = CONTENT_PATTERN.matcher(xml);
					if (m.matches()) {
						content = m.group(1);
						reporter.incrCounter(HADOOP_COUNTERS.OK_ARTICLE, 1);
						output.collect(new Text(title),
						    new Text(String.format("%s\t%s", "", processWikiText(content))));
					}
				}
			} else {
				reporter.incrCounter(HADOOP_COUNTERS.NO_TITLE, 1);
			}
		} catch (Exception e) {
			reporter.incrCounter(HADOOP_COUNTERS.MAP_EXCEPTION, 1);
		}
	}

	// Just for testing.
	public static void main(String[] args) throws Exception {
		ArticleTokenizerMapper obj = new ArticleTokenizerMapper();
		obj.configure(null);
		Scanner sc = new Scanner(new File("/tmp/dump.txt")).useDelimiter("\\Z");
		String xml = sc.next();
		Matcher m = CONTENT_PATTERN.matcher(xml);
		String content = m.group(1);
		System.out.println(obj.processWikiText(content));
		sc.close();
	}

}
