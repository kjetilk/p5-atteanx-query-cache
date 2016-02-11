package AtteanX::Model::SPARQLCache::LDF;


use v5.14;
use warnings;

use Moo;
use Types::Standard qw(InstanceOf);
use Class::Method::Modifiers;
use List::MoreUtils qw(any);
use namespace::clean;

extends 'AtteanX::Model::SPARQLCache';
with 'MooX::Log::Any';

has 'ldf_store' => (is => 'ro',
						  isa => InstanceOf['AtteanX::Store::LDF'],
						  required => 1);


around 'cost_for_plan' => sub {
	my $orig = shift;
	my @params = @_;
	my $self	= shift;
 	my $plan	= shift;
 	my $planner	= shift;
	my @passthroughs = qw/Attean::Plan::Table Attean::Plan::Quad/;
	my $cost = $orig->(@params);
	if ($self->log->is_debug) {
		my $logcost = $cost || 'not defined';
		$self->log->debug('Cost for original plan \'' . ref($plan) . "' was $logcost.");
	}
	if ($plan->isa('AtteanX::Store::LDF::Plan::Triple')) {
		$cost = $self->ldf_store->cost_for_plan($plan);
		return $cost;
	} elsif ($cost && any { $plan->isa($_) } @passthroughs) {
		# In here, we just pass the plans that probably do not need
		# balancing against others
		$self->log->debug("Use original's cost for '" . ref($plan) . "'");
		return $cost;
	} 
		# This is where the plans that needs to be balanced against LDFs go
		if ($plan->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
			if ($cost <= 1000 && (scalar(@{ $plan->children }) == 1)) {
				$self->log->trace("Set cost for single BGP SPARQL plan");
				$cost = 1001;
			} else {
				$cost = ($cost + 1) * 5;
			}
		}
			

#		$cost *= 10; # TODO: Just multiply by a factor for now...
	
	return $cost;
};



1;
