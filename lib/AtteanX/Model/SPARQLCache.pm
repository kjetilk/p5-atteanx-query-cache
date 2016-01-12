package AtteanX::Model::SPARQLCache;


use v5.14;
use warnings;

use Moo;
use Types::Standard qw(InstanceOf ArrayRef ConsumerOf HashRef);
use namespace::clean;
use Class::Method::Modifiers;

extends 'AtteanX::Model::SPARQL';
with 'MooX::Log::Any';

has 'cache' => (
					 is => 'ro',
					 isa => InstanceOf['CHI::Driver'],
					 required => 1
					);

# Override the store's planner, to take back control
sub plans_for_algebra {
	return;
}

around 'cost_for_plan' => sub {
	my $orig = shift;
	my @params = @_;
 	my $self	= shift;
 	my $plan	= shift;
 	my $planner	= shift;
	my $cost = $orig->(@params) || $planner->cost_for_plan($plan, $self);;
	warn $plan->as_string;
	$self->log->debug("Cost for original plan were $cost");
	if ($plan->isa('Attean::Plan::Table')) {
 		return 2;
	} elsif ($plan->isa('Attean::Plan::Quad')) {
 		return 100000;
 	} else {
		my $bgps = scalar $plan->subpatterns_of_type('AtteanX::Store::SPARQL::Plan::BGP');
		if ($bgps > 1) {
			# Penalize plans with more BGPs
			warn "DAAHUT: $bgps";# . Data::Dumper::Dumper(\@bgps);;
			return ($cost * 2 * $bgps); # TODO: What if parent model has costs for things we have?
		}
	}
 	return;
};

sub is_cached {
	my $self = shift;
	my $keypattern = shift;
	return $self->cache->is_valid($keypattern);
}


1;
