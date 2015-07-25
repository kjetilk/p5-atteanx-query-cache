package AtteanX::Model::SPARQLCache;


use v5.14;
use warnings;

use Moo;
use Types::Standard qw(InstanceOf ArrayRef ConsumerOf HashRef);
use Scalar::Util qw(reftype);
use namespace::clean;

has 'store'    => (
						 is => 'ro',
						 isa => InstanceOf['AtteanX::Store::SPARQL'],
						 required => 1,
						 handles => { size => 'size' ,
										  get_quads => 'get_triples',
										  count_quads => 'count_triples',
										  cost_for_plan => 'cost_for_plan' }
						);


with 'Attean::API::Model', 'Attean::API::CostPlanner';


has 'cache' => (
					 is => 'ro',
					 isa => InstanceOf['CHI::Driver'],
					 required => 1
					);

sub get_graphs {
	return Attean::ListIterator->new();
}


sub plans_for_algebra {
	return; # For now, just make access_plan find the plans
	my $self	= shift;
	my $algebra	= shift;
	if ($algebra->isa('Attean::Algebra::BGP') && scalar $algebra->elements > 1) {
		return AtteanX::Store::SPARQL::Plan::BGP->new(algebra => $algebra,
																	 in_scope_variables => [$algebra->in_scope_variables],
																	 distinct => 0); # TODO: Fix
	}
	return;
}

=item C<< get_quads ( $subject, $predicate, $object, $graph ) >>

Returns an L<Attean::API::Iterator> for quads in the model that match the
supplied C<< $subject >>, C<< $predicate >>, C<< $object >>, and C<< $graph >>.
Any of these terms may be undefined or a L<Attean::API::Variable> object, in
which case that term will be considered as a wildcard for the purposes of
matching.

The returned iterator conforms to both L<Attean::API::Iterator> and
L<Attean::API::QuadIterator>.

=cut


1;
