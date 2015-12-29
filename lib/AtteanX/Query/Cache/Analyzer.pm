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

has 'graph' => (is => 'ro', isa => InstanceOf['Attean::IRI'], default => iri('http://example.invalid'));

sub analyze {
	my $self = shift;
	my $parser = AtteanX::Parser::SPARQL->new();
	my ($algebra) = $parser->parse_list_from_bytes($self->query, $self->base_uri); # TODO: this is a bit of cargocult
	my %costs;
	my $planner = AtteanX::Query::Cache::Analyzer::QueryPlanner->new;
	foreach my $bgp ($algebra->subpatterns_of_type('Attean::Algebra::BGP')) {
		foreach my $triple (@{ $bgp->triples }) { # TODO: May need quads
			next if ($model->is_cached($triple));
			my $key = $triple->canonicalize->as_string;
			my $plan = $planner->plan_for_algebra($algebra, $model, [$self->graph]);
			$costs{$key} = $planner->cost_for_plan($plan);
		}
	}
}

1;
