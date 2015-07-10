#!/usr/bin/python

import json, sys, errno, codecs, re, HTMLParser
from collections import defaultdict

# We want to read and write unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

HREF_PATTERN = '<a href="/wiki/'
TAG_REGEX = re.compile(r'<a href="/wiki/([^"]*)" title="([^"]*)(.*)">')
SPECIAL_REGEX = re.compile(r'(Wikipedia|Special|File|Template|Help|Talk|User):[^_]|.*_talk:[^_]')
parser = HTMLParser.HTMLParser()

def normalize(title):
  title = unicode(parser.unescape(title))
  title = title.replace(' ', '_')
  return title

def process_line(title, html):
  pos = defaultdict(list)
  size = 0
  i = 0
  last_was_space = False
  while i < len(html):
    # Collapse multiple spaces.
    if last_was_space and (html[i] == ' ' or html[i] == '\n'):
      i += 1
    # Match links.
    elif html[i:(i+len(HREF_PATTERN))] == HREF_PATTERN:
      tag_end = html.index('>', i) + 1
      tag = html[i:tag_end]
      m = TAG_REGEX.match(tag)
      if m is not None:
        target = normalize(m.group(2))
        # Ignore links to special pages.
        if (SPECIAL_REGEX.match(target) is None):
          pos[target].append(str(size))
      i = tag_end
    # Ignore HTML tags other than links.
    elif html[i] == '<':
      i = html.index('>', i) + 1
    # Increase the count of printable characters.
    else:
      #sys.stdout.write(html[i])
      last_was_space = (html[i] == ' ' or html[i] == '\n')
      size += 1
      i += 1
  for target in pos.keys():
    # Ignore self-links.
    if target != title:
      print '\t'.join([title, target, str(size), ','.join(pos[target])])

if __name__ == '__main__':

  for line in sys.stdin:
    tokens = line.split('\t')
    if (len(tokens) >= 2):
      source = tokens[0]
      try:
        obj = json.loads(tokens[1])
        try:
          # This loop should only ever go through one iteration. To be sure, break after the first.
          for obj2 in obj['query']['pages'].values():
            title = normalize(obj2['title'])
            html = obj2['revisions'][0]['*']
            process_line(title, html)
            break
        except KeyError:
          pass
          # sys.stderr.write(u"JSON doesn't have all required fields: {}\n".format(source))
      except ValueError:
        pass
        # sys.stderr.write(u'Illegal JSON: {}\n'.format(tokens[1]))

  # For testing with HTML rather than JSON as input.
  # for line in sys.stdin:
  #   process_line('TEST', line)
