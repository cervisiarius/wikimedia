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

use Digest::MD5 qw(md5 md5_hex md5_base64);

sub is_spider {
  my $ua_string = shift;
  return $ua_string =~ m{[Bb]ot|[Ss]pider|WordPress|AppEngine|AppleDictionaryService|Python-urllib|python-requests|Google-HTTP-Java-Client|[Ff]acebook|[Yy]ahoo|RockPeaks|^Java/1\\.|^curl|^PHP/|^-$|^$};
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
      # Send the marker that the URL has been seen as a referer.
      if ($referer =~ m{^https?://(www\.)?simtk\.org(/.*)}) {
        print "$2\tSEEN_AS_REFERER\n";
      }
      # Filter and send this row. The first occurrence of $url is as a key, the second one, as part
      # of the value.
      if (!is_spider($user_agent) && $http_status =~ '^(200|302|304)$') {
        print join("\t", $url, md5_hex("$ip|$user_agent"), $date, $http_status, $url, $referer, $user_agent) . "\n";
      }
    }
  }
}
