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

my $parser = HTTP::UA::Parser->new();

   print $r->ua->toString();         # -> "Safari 5.0.1"
    print $r->ua->toVersionString();  # -> "5.0.1"
    print $r->ua->family;             # -> "Safari"
    print $r->ua->major;              # -> "5"
    print $r->ua->minor;              # -> "0"
    print $r->ua->patch;              # -> "1"
    
    print $r->os->toString();         # -> "iOS 5.1"
    print $r->os->toVersionString();  # -> "5.1"
    print $r->os->family              # -> "iOS"
    print $r->os->major;              # -> "5"
    print $r->os->minor;              # -> "1"
    print $r->os->patch;              # -> undef
    
    print $r->device->family;         # -> "iPhone"

    
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
