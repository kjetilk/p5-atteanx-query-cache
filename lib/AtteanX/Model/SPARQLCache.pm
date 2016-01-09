package AtteanX::Model::SPARQLCache;


use v5.14;
use warnings;

use Moo;
use Types::Standard qw(InstanceOf ArrayRef ConsumerOf HashRef);
use namespace::clean;

extends 'AtteanX::Model::SPARQL';

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
	my $joinfactor = ($plan->isa('Attean::Plan::HashJoin')) ? 9 : 10; # Consistently prefer HashJoins unless treated specially
	if ($plan->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
		# BGPs should have a cost proportional to the number of triple patterns,
		# but be much more costly if they contain a cartesian product.
		if ($plan->children_are_variable_connected) {
			return 10 * scalar(@{ $plan->children });
		} else {
			return 100 * scalar(@{ $plan->children });
		}
# 	} elsif ($plan->does('Attean::API::Plan::Join')) {
# 		if (${$plan->children}[0]->isa('Attean::Plan::Table') && ${$plan->children}[1]->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
# 			my $bgpcost	= $planner->cost_for_plan(${$plan->children}[1], $self);
# 			my $cost	= int($bgpcost * $joinfactor / 20);
# # 			say "1 Join costs: $bgpcost => $cost\n";
# 			return $cost;
# 		} elsif (${$plan->children}[1]->isa('Attean::Plan::Table') && ${$plan->children}[0]->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
# 			my $bgpcost	= $planner->cost_for_plan(${$plan->children}[0], $self);
# 			my $cost	= int($bgpcost * $joinfactor / 15);
# # 			say "2 Join costs: $bgpcost => $cost\n";
# 			return $cost;
# 		}
	} elsif ($plan->isa('Attean::Plan::Table')) {
 		return 2;
	} elsif ($plan->isa('Attean::Plan::Quad')) {
 		return 100000;
 	}
 	return;
}

sub is_cached {
	my $self = shift;
	my $keypattern = shift;
	return $self->cache->is_valid($keypattern);
}


1;
