#!/usr/bin/python

import os, sys, codecs, re, math
from collections import defaultdict

# We want to write unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

DATADIR = os.environ['HOME'] + '/wikimedia/trunk/data/health/'
KERNEL_WIDTH = 100

def normal(d):
  return math.exp(-d*d/(2.0*KERNEL_WIDTH*KERNEL_WIDTH))

f = codecs.open(DATADIR + 'Electronic_cigarette_20150112.html', 'r', 'utf8')
html = f.read()
f.close()

# Count how often each link occurs.
link_counts = defaultdict(int)
for link, garbage in re.findall(r'<a href="/wiki/([^"]*?)(#[^"]*?)?" title=".*?".*?>', html):
  link_counts[link] += 1

# Read click volumes.
weights = defaultdict(float)
f = codecs.open(DATADIR + 'clicktream_Electronic_cigarette.tsv', 'r', 'utf8')
for l in f:
  tokens = l.split('\t')
  denom = link_counts[tokens[4]] if link_counts[tokens[4]] > 0 else 1
  weights[tokens[4]] = float(tokens[2]) / denom
f.close()
# Normalize.
s = sum(weights.values())
for k in weights.keys():
  weights[k] = weights[k]/s

# Split HTML tags off from normal text by a whitespace.
html = re.sub(r'<', r' <', html)
html = re.sub(r'>', r'> ', html)
html = re.sub(r'\s+', r' ', html)
# Fix links.
html = re.sub(r' (href|src)="//', r' \1="http://', html)
html = re.sub(r' srcset=".*?"', lambda m: m.group().replace('//', 'http://'), html)

# Grab body.
m = re.search(r'(.*)(<body.*?>.*</body>)(.*)', html)
header, body, footer = m.group(1), m.group(2), m.group(3)
# Fix more links.
header = re.sub(r' (href|src)="/([^/])', r' \1="http://en.wikipedia.org/\2', header)
footer = re.sub(r' (href|src)="/([^/])', r' \1="http://en.wikipedia.org/\2', footer)
# Attach links as non-HTML tag to first token in anchor text.
body = re.sub(r'<a href="/wiki/([^"]*?)(#[^"]*?)?" title=".*?".*?> *(.*?) *</a>',
  lambda m: u'{}::{}'.format(m.group(1), m.group(3).replace(' ', u'\u00A0')), body)
# Whitespace-tokenize.
tokens = re.split(r' +', re.sub(r'<.*?>', lambda m: m.group().replace(' ', u'\u00A0'), body))

aggr = [0] * len(tokens)

for i in range(0, len(tokens)):
  if '::' in tokens[i]:
    link, anchor = tokens[i].split('::')[0:2]
    d = 0
    for j in range(i, -1, -1):
      if not(tokens[j].startswith('<') and tokens[j].endswith('>')):
        aggr[j] += weights[link] * normal(d)
        d += 1
    d = 1
    for j in range(i+1, len(tokens)):
      if not(tokens[j].startswith('<') and tokens[j].endswith('>')):
        aggr[j] += weights[link] * normal(d)
        d += 1

# Normalize.
m = max(aggr)
aggr = [int((1-a/m)*255) for a in aggr]

for i in range(0, len(tokens)):
  tokens[i] = tokens[i].replace(u'\u00A0', ' ')
  if '::' in tokens[i]:
    link, anchor = tokens[i].split('::')[0:2]
    tokens[i] = '<a href="http://en.wikipedia.org/wiki/{}">{}</a>'.format(link, anchor)
  col = hex(aggr[i])[2:]
  if len(col) == 1: col = '0' + col
  tokens[i] = u'<span style="background:#ff{0}{0}">{1} </span>'.format(col, tokens[i])

body = ''.join(tokens)
html = header + body + footer

f = codecs.open(DATADIR + 'Electronic_cigarette_20150112_COLORED.html', 'w', 'utf8')
f.write(html)
f.close()
