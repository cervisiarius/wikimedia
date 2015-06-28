#!/usr/bin/perl

my $url_ok = 0;
my @values = ();
my $last_key = '';

while (my $line = <STDIN>) {
  chomp $line;
  my ($key, $value) = split($line, /\t/, 2);
  # When we see a new key, flush the buffer.
  if ($key ne $last_key && $last_key ne '') {
    if ($url_ok) {
      foreach my $value (@values) {
        print "$value\n";
      }
    }
    $url_ok = 0;
    undef @values;
  }
  if ($value eq 'SEEN_AS_REFERER') {
    $url_ok = 1;
  } else {
    push(@values, $value);
  }
  $last_key = $key;
}

# Process the last line.
if ($url_ok) {
  foreach my $value (@values) {
    print "$value\n";
  }
}
