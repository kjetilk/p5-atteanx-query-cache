package AtteanX::Model::SPARQLCache::LDF;


use v5.14;
use warnings;

use Moo;
use Types::Standard qw(InstanceOf ArrayRef ConsumerOf HashRef);
use namespace::clean;

extends 'AtteanX::Model::SPARQLCache';

has 'ldf_store' => (is => 'ro',
						  isa => InstanceOf['AtteanX::Store::LDF'],
						  required => 1);


sub cost_for_plan {
	my $self	= shift;
 	my $plan	= shift;
 	my $planner	= shift;
	my $cost;
	return $plan->cost if ($plan->has_cost);
	if ($plan->isa('AtteanX::Store::LDF::Plan::Triple')) {
		$cost = $self->ldf_store->cost_for_plan($plan);
		$plan->cost($cost);
		return $cost;
	}
	return;
}



1;
