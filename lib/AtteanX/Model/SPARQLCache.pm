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
 	if ($plan->does('Attean::API::Plan::Join') && ${$plan->children}[0]->isa('Attean::Plan::Quad') && ${$plan->children}[1]->isa('Attean::Plan::Quad')) {
 		return 10000;
 	} elsif ($plan->isa('Attean::Plan::Table')) {
 		return 2;
# 	} elsif ($plan->isa('Attean::Plan::HashJoin')) {
# 		return 2;
# 	} elsif ($plan->isa('Attean::Plan::NestedLoopJoin')) {
# 		return 3;
 	}
 	return;
}

1;
