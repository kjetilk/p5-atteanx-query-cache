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
	}
	if ($cost && any { $plan->isa($_) } @passthroughs) {
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
		return $cost;
	}
	if ($plan->does('Attean::API::Plan::Join')) {
		# Then, penalize the plan by the number of LDFs
		my $countldfs = scalar $plan->subpatterns_of_type('AtteanX::Store::LDF::Plan::Triple');
		return unless ($countldfs);
		unless ($cost) {
			my @children	= @{ $plan->children };
			if ($plan->isa('Attean::Plan::NestedLoopJoin')) {
				my $lcost		= $planner->cost_for_plan($children[0], $self);
				my $rcost		= $planner->cost_for_plan($children[1], $self);
				if ($lcost == 0) {
					$cost	= $rcost;
				} elsif ($rcost == 0) {
					$cost	= $lcost;
				} else {
					$cost	= $lcost * $rcost;
				}
				$cost	*= 10 unless ($plan->children_are_variable_connected);
			} elsif ($plan->isa('Attean::Plan::HashJoin')) {
				my $joined		= $plan->children_are_variable_connected;
				my $lcost		= $planner->cost_for_plan($children[0], $self);
				my $rcost		= $planner->cost_for_plan($children[1], $self);
				$cost	= ($lcost + $rcost);
				$cost	*= 100 unless ($plan->children_are_variable_connected);
			}
		}
		$cost *= $countldfs;
	}

	# Now, penalize plan if any SPARQLBGP has a common variable with a LDFTriple
	my %bgpvars;
	my %ldfvars;
	my $shared = 0;
	$plan->walk(prefix => sub {
						my $node = shift;
						if ($node->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
							map { $bgpvars{$_} = 1 } @{$node->in_scope_variables};
						}
						elsif ($node->isa('AtteanX::Store::LDF::Plan::Triple')) {
							map { $ldfvars{$_} = 1 } @{$node->in_scope_variables};
							# TODO: A single loop should be sufficient
						}
						foreach my $lid (keys(%ldfvars)) {
							if ($bgpvars{$lid}) {
								$shared = 1;
								last;
								# TODO: Jump out of the walk here
							}
						}
					});
	  $cost += 1000 if ($shared);
#		$cost *= 10; # TODO: Just multiply by a factor for now...
	
	return $cost;
};



1;
