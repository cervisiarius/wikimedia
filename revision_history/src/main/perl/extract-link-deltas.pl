#!/usr/bin/perl -CS

# NB: Note the -CS flag above; it is such that we read and write from STDIN and STDOUT in UTF-8 format.

# Important: this assumes the input to be grouped by page id and sorted by timestamp!

use HTML::Entities;
# Important: deal with unicode properly (for uppercasing etc.).
use utf8;

sub normalizeTitle {
	my $title = shift;
	# Trim.
	$title =~ s/^\s+|\s+$//g;
	# Sometimes there are too many brackets. Remove them.
	$title =~ s/^\[+|\]+$//g;
	$title = decode_entities($title);
	$title = ucfirst($title);
	$title =~ s/_/ /g;
	# Remove anchors.
	if ($title =~ /(.*?)\s*#/) {
		$title = $1;
	}
	return $title;
}

sub resolveRedirect {
	my $title = shift;
	if (defined $redirects{$title}) {
		return $redirects{$title};
	} else {
		return $title;
	}
}

sub filterTitle {
	my $title = shift;
	if (# Meta pages.
		$title =~ /^(Image|Media|Special|Talk|User|Wikipedia|File|MediaWiki|Template|Help|Book|Draft|Education Program|TimedText|Module|Wikt)( talk)?:/i
		# Language links:
		|| $title =~ /^[A-Za-z]{2,3}:\w/
		# Empty links.
		|| $title eq ''
		# Web links.
		|| $title =~ /^http:\/\/|^www\./i) {
		return 0;
	} else {
		return 1;
	}
}

# Load redirects.
sub loadRedirects {
	my %harshibarky = ();
	# Important: read in UTF-8 mode.
	open(RED, '-|:encoding(utf-8)', 'zcat /u/west1/wikimedia/trunk/data/redirects/enwiki_20141008_redirects.tsv.gz');
	while (my $line = <RED>) {
		chomp $line;
		# Use whitespace instead of underscore.
		$line =~ s/_/ /g;
		my ($title, $target) = split(/\t/, $line);
		$harshibarky{$title} = $target;
	}
	close(RED);
	return %harshibarky;
} 

print STDERR "Loading redirects... ";
%redirects = loadRedirects();
print STDERR "DONE\n";

my $page_prevId = '';
my %revision_prevLinks = ();
my $i = 0;

while (my $line = <STDIN>) {
	chomp $line;
	my ($page_id, $page_title, $page_redirectTitle, $page_isRedirect, $revision_id,
		$revision_parentid, $revision_timestamp, $revision_userid, $revision_username,
		$revision_length, $revision_links_string) = split(/\t/, $line);
	++$i;
	print STDERR "$i\t$page_id\n" if ($i % 100000 == 0);
	# New page, so reset the previous links.
	if ($page_prevId != $page_id) {
		%revision_prevLinks = ();
	}
	# Ignore redirect rows. We simply skip them, i.e., the previous link set is the one before
	# the redirect, rather than the single link contained in the redirect.
	if (!$page_isRedirect) {
		# Collect the links; normalize titles and resolve redirects; discard links to meta pages,
		# as well as links to other language versions; 
		my %revision_links = ();
		my @revision_links_array = split(/\|/, $revision_links_string);
		foreach my $link (@revision_links_array) {
			$link = normalizeTitle(resolveRedirect(normalizeTitle($link)));
			if (filterTitle($link) && ($link ne $page_title)) {
				$revision_links{$link} = 1;
			}
		}
		# Find the links that were added and deleted, w.r.t. the previous version.
		my @addLinks = ();
		my @delLinks = ();
		foreach my $curLink (keys %revision_links) {
			push(@addLinks, $curLink) if (!defined $revision_prevLinks{$curLink});
		}
		foreach my $prevLink (keys %revision_prevLinks) {
			push(@delLinks, $prevLink) if (!defined $revision_links{$prevLink});
		}
		# No need to output anything if links didn't change.
		if (scalar(@addLinks) > 0 || scalar(@delLinks) > 0)  {
			print join("\t", $page_id, $page_title, $revision_id, $revision_parentid,
				$revision_timestamp, $revision_userid, $revision_username, $revision_length,
				join('|', sort {lc $a cmp lc $b} @delLinks),
				join('|', sort {lc $a cmp lc $b} @addLinks)) . "\n";
		}
		# Remember these links as the previous values for the next step.
		%revision_prevLinks = %revision_links;
	}
	# Remember the currect values as previous values for the next step.
	$page_prevId = $page_id;
}
