#!/usr/bin/perl

# Apache log format (see section "Combined Log Format"): http://httpd.apache.org/docs/2.2/logs.html#accesslog
# e.g., 0.0.0.0 - - [29/Dec/2013:08:53:46 -0800] "GET /project/api.php/wiki-moin-sidebar?wiki_namespace=opensim HTTP/1.1" 200 4290 "-" "Python-urllib/2.4"
# Columns:
# (0) ip address
# (1) RFC 1413 identity (always "-")
# (2) userid (via HTTP auth)
# (3) time
# (4) request string
# (5) status code
# (6) response size
# (7) referer
# (8) user agent

use HTTP::UA::Parser;

my $ua_parser = HTTP::UA::Parser->new();

# 'Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25 (compatible; Googlebot-Mobile/2.1; +http://www.google.com/bot.html)'        

sub is_spider {
  $ua_parser->parse($_);
  return $ua_parser->device->family eq 'Spider';
}
    
my %month_map = (
  'Jan' => '01',
  'Feb' => '02',
  'Mar' => '03',
  'Apr' => '04',
  'May' => '05',
  'Jun' => '06',
  'Jul' => '07',
  'Aug' => '08',
  'Sep' => '09',
  'Oct' => '10',
  'Nov' => '11',
  'Dec' => '12'
  );

while (my $line = <STDIN>) {
  chomp $line;
  if ($line =~ m{(\S*) \S* \S* \[(.*)\] "GET (.*) HTTP/.*" (\d+) \d+ "(.*)" "(.*)"}) {
    my ($ip, $date, $url, $http_status, $referer, $user_agent) = ($1, $2, $3, $4, $5, $6);
    if ($date =~ m{(\d+)/(.+)/(....):(..:..:..) (-?\d+)}) {
      my ($day, $month, $year, $time, $zone) = ($1, $2, $3, $4, $5);
      $date = "$year-".$month_map{$month}."-$day\T$time";
      print join("\t", $ip, $date, $url, $http_status, $referer, $user_agent) . "\n";
      print $parser
    }
  }
}
