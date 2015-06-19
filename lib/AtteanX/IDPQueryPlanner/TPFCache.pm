use 5.010001;
use strict;
use warnings;


package AtteanX::IDPQueryPlanner::TPFCache;
use Class::Method::Modifiers;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001';

use Moo;
use Types::Standard qw(InstanceOf);
use Attean::RDF qw(triplepattern variable iri);
use Carp;

extends 'Attean::IDPQueryPlanner';

has cache => (is => 'ro',
				  isa => InstanceOf['CHI::Driver'],
				  required => 1
				 );

around 'access_plans' => sub {
	my $orig = shift;
	my @params = @_;
	my $self	= shift;
	my $model = shift;
	my $active_graphs	= shift;
	my $pattern	= shift;
	my $keypattern = $self->_normalize_pattern($pattern);
	my $cached = $self->cache->get($keypattern->tuples_string);
	if (defined($cached)) {
		# We found data in the cache
		my @vars	= $pattern->values_consuming_role('Attean::API::Variable');
		my $parser = Attean->get_parser('NTriples')->new;
		my @rows;
		if (ref($cached) eq 'ARRAY') {
			foreach my $row (@{$cached}) { # TODO: arbitrary terms
				my $term = $parser->parse_term_from_string($row);
				push(@rows, Attean::Result->new(bindings => { $vars[0]->value => $term }));
			}
		} elsif (ref($cached) eq 'HASH') {
			while (my($first, $second) = each(%{$cached})) {
				my $term1 = $parser->parse_term_from_string($first);
				foreach my $term (@{$second}) {
					my $term2 = $parser->parse_term_from_string($term);
					push(@rows, Attean::Result->new(bindings => {$vars[0]->value => $term1,
																				$vars[1]->value => $term2}));
				}
			}
		} else {
			croak 'Unknown data structure found in cache for key ' . $keypattern->tuples_string;
		}
		return Attean::Plan::Table->new( variables => \@vars,
													rows => \@rows,
													distinct => 0,
													in_scope_variables => [ map {$_->value} @vars],
													ordered => [] );
	} else {
		return $orig->(@params)
	}
	# Then check TPF and remote SPARQL
};


sub _normalize_pattern {
	my ($self, $pattern) = @_;
	my @keyterms = $pattern->values;
	my @varnames = $pattern->variables;
	my $i = 0;
	foreach my $term (@keyterms) {
		if ($term->is_variable) {
			$keyterms[$i] = variable($varnames[$i]); # Normalize variable names
		}
		$i++;
	}
	return triplepattern(@keyterms);
}

1;

__END__

=pod

=encoding utf-8

=head1 NAME

AtteanX::IDPQueryPlanner::TPFCache - Extending the query planner with cache and TPF support

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SEE ALSO

=head1 AUTHOR

Kjetil Kjernsmo E<lt>kjetilk@cpan.orgE<gt>.

=head1 COPYRIGHT AND LICENCE

This software is copyright (c) 2015 by Kjetil Kjernsmo.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.


=head1 DISCLAIMER OF WARRANTIES

THIS PACKAGE IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.

