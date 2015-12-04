use 5.010001;
use strict;
use warnings;


package AtteanX::IDPQueryPlanner::Cache;
use Class::Method::Modifiers;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001';

use Moo;
use Types::Standard qw(InstanceOf);
use Attean::RDF qw(triplepattern variable iri);
use Carp;

extends 'Attean::IDPQueryPlanner';

around 'access_plans' => sub {
	my $orig = shift;
	my @params = @_;
	my $self	= shift;
	my $model = shift;
	my $active_graphs	= shift;
	my $pattern	= shift;

	# First, add any plans coming from the original planner (which will
	# include queries to the remote SPARQL endpoint
	my @plans = $orig->(@params);
	my @vars	= $pattern->values_consuming_role('Attean::API::Variable');

	# Start checking the cache
	my $keypattern = $self->_normalize_pattern($pattern);
	my $cached = $model->cache->get($keypattern->tuples_string);
	if (defined($cached)) {
		# We found data in the cache
		my $parser = Attean->get_parser('NTriples')->new;
		my @rows;
		if (ref($cached) eq 'ARRAY') {
			# Then, the cache resulted from a TP with just one variable
			foreach my $row (@{$cached}) { # TODO: arbitrary terms
				my $term = $parser->parse_term_from_string($row);
				push(@rows, Attean::Result->new(bindings => { $vars[0]->value => $term }));
			}
		} elsif (ref($cached) eq 'HASH') {
			# Cache resulted from TP with two variables
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
		push(@plans, Attean::Plan::Table->new( variables => \@vars,
															rows => \@rows,
															distinct => 0,
															in_scope_variables => [ map {$_->value} @vars],
															ordered => [] ));
	}

	return @plans;
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

# Gather patterns into larger BGPs
around 'join_plans' => sub {
	my $orig = shift;
	my @params = @_;
	my $self	= shift;
	my $active_graphs	= shift;
	my $default_graphs	= shift;
	my $lplans			= shift;
	my $rplans			= shift;
	my @plans;
	foreach my $lhs (@{ $lplans }) {
		foreach my $rhs (@{ $rplans }) {
			if ($lhs->isa('Attean::Plan::Quad') &&
				 $rhs->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
				push(@plans, AtteanX::Store::SPARQL::Plan::BGP->new(quads => [$lhs, Attean::Plan::Quad->new($rhs->quads)]));
			}
			elsif ($rhs->isa('Attean::Plan::Quad') &&
					 $lhs->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
				push(@plans, AtteanX::Store::SPARQL::Plan::BGP->new(quads => [$rhs, Attean::Plan::Quad->new($lhs->quads)]));
			}
			elsif ($rhs->isa('AtteanX::Store::SPARQL::Plan::BGP') &&
					 $lhs->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
				push(@plans, AtteanX::Store::SPARQL::Plan::BGP->new(quads => [Attean::Plan::Quad->new($lhs->quads), Attean::Plan::Quad->new($rhs->quads)]));
			}
			elsif ($rhs->isa('Attean::Plan::Quad') &&
					 $lhs->isa('Attean::Plan::Quad')) {
				push(@plans, AtteanX::Store::SPARQL::Plan::BGP->new(quads => [$lhs, $rhs]));
			}
			elsif ($lhs->isa('Attean::Plan::Quad') && $rhs->does('Attean::API::Plan::Join')) {
				if (${$rhs->children}[0]>isa('Attean::Plan::Quad')) {
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(quads => [$lhs, ${$rhs->children}[0]]
																								 # TODO
																								);
					push(@plans, $orig->($self, $active_graphs, $default_graphs, [$new_bgp_plan], [${$rhs->children}[1]]));
				} elsif (${$rhs->children}[1]>isa('Attean::Plan::Quad')) {
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(quads => [$lhs, ${$rhs->children}[1]]
																								 # TODO
																								);
					push(@plans, $orig->($self, $active_graphs, $default_graphs, [$new_bgp_plan], [${$rhs->children}[0]]));
				} else {
					# If we get here, both children of $rhs are Table (if not, it is a bug)
					push(@plans, $orig->($self, $active_graphs, $default_graphs, [$lhs], [$rhs])); # TODO: Is this correct?
				}
			}
			elsif ($rhs->isa('Attean::Plan::Quad') && $lhs->does('Attean::API::Plan::Join')) {
				if (${$lhs->children}[0]>isa('Attean::Plan::Quad')) {
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(quads => [$rhs, ${$lhs->children}[0]]
																								 # TODO
																								);
					push(@plans, $orig->($self, $active_graphs, $default_graphs, [$new_bgp_plan], [${$lhs->children}[1]]));
				} elsif (${$lhs->children}[1]>isa('Attean::Plan::Quad')) {
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(quads => [$rhs, ${$lhs->children}[1]]
																								 # TODO
																								);
					push(@plans, $orig->($self, $active_graphs, $default_graphs, [$new_bgp_plan], [${$lhs->children}[0]]));
				} else {
					# If we get here, both children of $rhs are Table (if not, it is a bug)
					push(@plans, $orig->($self, $active_graphs, $default_graphs, [$rhs], [$lhs])); # TODO: Is this correct?
				}
			}
		}
	}
	unless (@plans) {
		@plans = $orig->(@params);
	}
	return @plans;
};


1;

__END__

=pod

=encoding utf-8

=head1 NAME

AtteanX::IDPQueryPlanner::Cache - Extending the query planner with cache and SPARQL support

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

