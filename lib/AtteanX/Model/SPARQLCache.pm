package AtteanX::Model::SPARQLCache;


use v5.14;
use warnings;

use Moo;
use Types::Standard qw(InstanceOf ArrayRef ConsumerOf HashRef);
use Scalar::Util qw(reftype);
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

sub cost_for_plan { # TODO: Do this for real
 	my $self	= shift;
 	my $plan	= shift;
	my $joinfactor = ($plan->isa('Attean::Plan::HashJoin')) ? 9 : 10; # Consistently prefer HashJoins unless treated specially
	if ($plan->does('Attean::API::Plan::Join')) {
		if (${$plan->children}[0]->isa('Attean::Plan::Quad') && ${$plan->children}[1]->isa('Attean::Plan::Quad')) {
			return $joinfactor * 1000
		} elsif (${$plan->children}[0]->isa('Attean::Plan::Table') && ${$plan->children}[1]->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
			return int(@{$plan->children}[1]->cost * $joinfactor / 20);
		} elsif (${$plan->children}[1]->isa('Attean::Plan::Table') && ${$plan->children}[0]->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
			return int(@{$plan->children}[0]->cost * $joinfactor / 15);
		}
	} elsif ($plan->isa('Attean::Plan::Table')) {
 		return 2;
 	}
 	return;
}

1;
