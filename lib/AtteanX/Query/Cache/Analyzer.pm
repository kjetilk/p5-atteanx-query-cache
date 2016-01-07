package AtteanX::Query::Cache::Analyzer;

use 5.010001;
use strict;
use warnings;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001';

use Moo;
use Attean::RDF qw(triplepattern variable iri);
use Types::Standard qw(Str Int InstanceOf);
use Types::URI -all;
use AtteanX::Parser::SPARQL;
use AtteanX::Query::Cache::Analyzer::Model;
use AtteanX::QueryPlanner::Cache;
use AtteanX::Query::Cache::Analyzer::QueryPlanner;

use Carp;

has 'query' => (is => 'ro', required => 1, isa => Str);
has 'base_uri' => (is => 'ro', default => 'http://default.invalid/');

has 'model' => (is => 'ro', isa => InstanceOf['AtteanX::Query::Cache::Analyzer::Model'], required => 1);

has 'graph' => (is => 'ro', isa => InstanceOf['Attean::IRI'], default => sub { return iri('http://example.invalid')});

has 'improvement_threshold' => (is => 'ro', isa => Int, default => '10');
has 'improvement_top' => (is => 'ro', isa => Int, default => '3');

with 'MooX::Log::Any';


sub best_cost_improvement {
	my $self = shift;
	my $parser = AtteanX::Parser::SPARQL->new();
	my ($algebra) = $parser->parse_list_from_bytes($self->query, $self->base_uri); # TODO: this is a bit of cargocult
	# First, we find the cost of the plan with the current cache:
	my $curplanner = AtteanX::QueryPlanner::Cache->new;
	my $curplan = $curplanner->plan_for_algebra($algebra, $self->model, [$self->graph]);
	my $curcost = $curplanner->cost_for_plan($curplan, $self->model);
	$self->log->trace("Cost of incumbent plan: $curcost");
	my %costs;
	my %triples;
	my $percentage = 1-($self->improvement_threshold/100);
	my $planner = AtteanX::Query::Cache::Analyzer::QueryPlanner->new;
	foreach my $bgp ($algebra->subpatterns_of_type('Attean::Algebra::BGP')) {
		foreach my $triple (@{ $bgp->triples }) { # TODO: May need quads
			my $key = $triple->canonicalize->tuples_string;
			next if ($self->model->is_cached($key));
			$self->model->try($key);
			if ($self->log->is_trace) {
				foreach my $plan ($planner->plans_for_algebra($algebra, $self->model, [$self->graph])) {
					my $cost = $planner->cost_for_plan($plan, $self->model);
					$self->log->trace("Cost $cost for:\n" . $plan->as_string);
				}
			}
			my $plan = $planner->plan_for_algebra($algebra, $self->model, [$self->graph]);
			$self->log->debug("Alternative plan after fetching $key:\n" . $plan->as_string);
			$costs{$key} = $planner->cost_for_plan($plan, $self->model);
			$self->log->info("Triple $key has cost $costs{$key}, current $curcost");
			if ($costs{$key} < $curcost * $percentage) {
				$triples{$key} = $triple;
			}
		}
	}
	no sort 'stable';
	my @worthy = map { $triples{$_} } sort {$costs{$a} <=> $costs{$b}} keys(%triples);
	return splice(@worthy,0, $self->improvement_top-1);
}

1;
