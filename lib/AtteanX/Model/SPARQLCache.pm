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

1;
