use 5.010001;
use strict;
use warnings;


package AtteanX::Query::Cache::Analyzer::QueryPlanner;
use Class::Method::Modifiers;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001';

use Moo;
use Attean::RDF qw(triplepattern variable iri);
use Carp;

extends 'Attean::QueryPlanner';

after 'access_plans' => sub {
	my $orig = shift;
	my @params = @_;
	my $self	= shift;
	my $model = shift;
	my $active_graphs	= shift;
	my $pattern	= shift;
	# First, add any plans coming from the original planner (which will
	# include queries to the remote SPARQL endpoint
	my @plans = $orig->(@params);
	my @vars	= $pattern->values_consuming_role('Attean::API::Variable');
	
	# Start checking the cache
	my $keypattern = $self->_normalize_pattern($pattern);
	my $cached = $model->cache->get($keypattern->tuples_string);
	if (defined($cached)) {
		$self->log->debug("Already accounted for by cache: " . $keypattern->tuples_string);
		return @plans;
	} else {
		
	}
};

1;
