#!/usr/bin/perl -CS

# NB: Note the -CS flag above; it is such that we read and write from STDIN and STDOUT in UTF-8 format.

use HTML::Entities;

my $page_title = '';
my $page_id = -1;
my $page_ns = -1;
my $page_redirecttitle = '';

my $revision_id = -1;
my $revision_parentid = -1;
my $revision_timestamp = '';
my $revision_text = '';
my $revision_userid = -1;
my $revision_username = '';

# This must be set to 1 when we're inside the (multi-line) text tag.
my $collect_text_flag = 0;
# This is just for debug output.
my $new_page_flag = 0;
my $page_counter = 0;

sub resetPageVars {
	$page_title = '';
	$page_id = -1;
	$page_ns = -1;
	$page_redirecttitle = '';
}

sub resetRevisionVars {
	$revision_id = -1;
	$revision_parentid = -1;
	$revision_timestamp = '';
	$revision_text = '';
	$revision_userid = -1;
	$revision_username = '';
}

sub extractLinks {
	my $text = shift;
	# Decode HTML entities.
	$text =~ s/&amp;/&/g;
	$text = decode_entities($text);
	my @link_tags = ($text =~ /\[\[(.*?)\]\]/g);
	my %links_hash = ();
	foreach my $tag (@link_tags) {
		if ($tag =~ /\s*(.*?)\s*\|/) {
			$tag = $1;
		}
		$links_hash{$tag} = 1;
	}
	my @links = sort {lc $a cmp lc $b} (keys %links_hash);
	return \@links;
}

sub printRevision {
	my $links_ref = extractLinks($revision_text);
	print join("\t", $page_id, $page_title, $page_redirecttitle,
		($revision_text =~ /^\s*#REDIRECT/i ? 'REDIRECT' : ''),
		$revision_id, $revision_parentid,
		$revision_timestamp, $revision_userid, $revision_username, length($revision_text),
		join('|', @{$links_ref}));
	print "\n";
	#print "------------------------------------\n";
}

while (my $line = <STDIN>) {
	# A new page begins.
	if ($line =~ m{^\s*<page>\s*$}) {
		$new_page_flag = 1;
		++$page_counter;
	# The current page entry is over.
	} elsif ($line =~ m{^\s*</page>\s*$}) {
		resetPageVars();
		resetRevisionVars();
	# The current revision entry is over.
	} elsif ($line =~ m{^\s*</revision>\s*$}) {
		# Print an output row, but only if it's in the main namespace.
		printRevision() if ($page_ns == 0);
		resetRevisionVars();
	# This must be the first check, since the skip flag changes here.
	} elsif ($line =~ m{^\s*<ns>(.*)</ns>\s*$}) {
		$page_ns = $1;
		if ($new_page_flag == 1) {
			$new_page_flag = 0;
			print STDERR "$page_counter\tns $page_ns\t$page_title\n";
		}
	} elsif ($line =~ m{^\s*<title>(.*)</title>\s*$}) {
		$page_title = $1;
	} elsif ($line =~ m{^\s*<redirect title="(.*)" />\s*$}) {
		$page_redirecttitle = $1;
	} elsif ($line =~ m{^\s*<parentid>(.*)</parentid>\s*$}) {
		$revision_parentid = $1;
	} elsif ($line =~ m{^\s*<id>(.*)</id>\s*$}) {
		if ($page_id >= 0) {
			# Page id defined and revision id defined => this is the contributor id.
			if ($revision_id >= 0) { $revision_userid = $1; }
			# Page id defined and revision id undefined => this is the revision id.
			else { $revision_id = $1; }
		# Page id undefined => this is the revision id.
		} else {
			$page_id = $1;
		}
	} elsif ($line =~ m{^\s*<timestamp>(.*)</timestamp>\s*$}) {
		$revision_timestamp = $1;
	} elsif ($line =~ m{^\s*<username>(.*)</username>\s*$}) {
		$revision_username = $1;
	} elsif ($line =~ m{^\s*<ip>(.*)</ip>\s*$}) {
		$revision_username = $1;
	} elsif ($line =~ m{^\s*<text xml:space="preserve">(.*)}) {
		$revision_text = $1;
		$collect_text_flag = 1;
	} elsif ($line =~ m{(.*)</text>\s*$}) {
		$revision_text .= $1;
		$collect_text_flag = 0;
	} elsif ($collect_text_flag == 1) {
		$revision_text .= $line;
	}
}
