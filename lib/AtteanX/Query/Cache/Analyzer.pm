package AtteanX::Query::Cache::Analyzer;

use 5.010001;
use strict;
use warnings;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001';

use Moo;
use Attean::RDF qw(triplepattern variable iri);
use Types::Standard qw(Str InstanceOf);
use Types::URI -all;
use AtteanX::Parser::SPARQL;
use AtteanX::Query::Cache::Analyzer::Model;

use Carp;

has 'query' => (is => 'ro', required => 1, isa => Str);
has 'base_uri' => (is => 'ro', default => 'http://default.invalid/');

has 'model' => (is => 'ro', isa => InstanceOf['AtteanX::Query::Cache::Analyzer::Model'], required => 1);

sub analyze {
	my $self = shift;
	my $parser = AtteanX::Parser::SPARQL->new();
	my ($algebra) = $parser->parse_list_from_bytes($self->query, $self->base_uri); # TODO: this is a bit of cargocult
#	warn Data::Dumper::Dumper($algebra);
	my @data = $algebra->subpatterns_of_type('Attean::TriplePattern');
	warn Data::Dumper::Dumper(@data);
	foreach my $triple ($algebra->subpatterns_of_type('Attean::API::TriplePattern')) { # TODO: May need quads
		my $bgp = Attean::Algebra::BGP->new(triples => $triple);
		warn "FOO: " . $bgp->canonical_bgp_with_mapping;
	}
}

1;
