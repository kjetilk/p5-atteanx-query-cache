package AtteanX::Model::SPARQLCache;


use v5.14;
use warnings;

use Moo;
use Types::Standard qw(InstanceOf);
use namespace::clean;
use List::Util qw(min);

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

sub cost_for_plan {
 	my $self	= shift;
 	my $plan	= shift;
 	my $planner	= shift;
#	warn $plan->as_string;
	if ($plan->isa('Attean::Plan::Table')) {
 		return 2;
	} elsif ($plan->isa('Attean::Plan::Quad')) {
 		return 100000;
	} elsif ($plan->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
		# BGPs should have a cost proportional to the number of triple patterns,
		# but be much more costly if they contain a cartesian product.
		$self->log->trace('Estimating cost for single BGP');
		if ($plan->children_are_variable_connected) {
			return 20 * scalar(@{ $plan->children });
		} else {
			return 200 * scalar(@{ $plan->children });
		}
 	} elsif ($plan->does('Attean::API::Plan::Join')) {
		my @bgps = $plan->subpatterns_of_type('AtteanX::Store::SPARQL::Plan::BGP');
		my $countbgps = scalar(@bgps);
		return unless $countbgps;
		# Now, we have SPARQLBGPs as subplans, which is usually not wanted
		my @children	= @{ $plan->children };
		if ($self->log->is_trace) {
			$self->log->trace("Found $countbgps SPARQL BGP subplans, immediate children are of type " . join(', ', map {ref} @children))
		}
		my $cost = 0;
		# The below code is from Attean::API::SimpleCostPlanner
		if ($plan->isa('Attean::Plan::NestedLoopJoin')) {
			my $lcost		= $planner->cost_for_plan($children[0], $self);
			my $rcost		= $planner->cost_for_plan($children[1], $self);
			unless (defined($lcost)) {
				$lcost = 5;
				die "A\n" . $children[0]->as_string;
			}
			unless (defined($rcost)) {
				$rcost = 5;
				die "B\n" . $children[1]->as_string;
			}

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
			if (($children[1]->isa('Attean::Plan::HashJoin'))) {
			#	$DB::single=1;
				my $tmp = 0;
				foreach my $gc (@{$children[1]->children}) {
					use Scalar::Util qw(blessed);
					if (blessed($gc) && $gc->isa('Attean::Plan::Table')) {
						$tmp++;
					}
				}
				$DB::single = 1 if $tmp >= 2;
			}
			my $lcost		= $planner->cost_for_plan($children[0], $self);
			my $rcost		= $planner->cost_for_plan($children[1], $self);
			unless (defined($lcost)) {
				$lcost = 5;
				die "C\n" .  $children[0]->as_string;
			}
			unless (defined($rcost)) {
				$rcost = 5;
				die "D\n" . $children[1]->as_string;
			}

			$cost	= ($lcost + $rcost);
			$cost	*= 100 unless ($plan->children_are_variable_connected);
		}
		if ($cost) {
			$cost *= $countbgps * 1.2;
			$cost = min($cost, 1_000_000_000);
			return int($cost);
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
