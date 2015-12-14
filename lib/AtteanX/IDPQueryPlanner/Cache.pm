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
use AtteanX::Store::SPARQL::Plan::BGP;

extends 'Attean::IDPQueryPlanner';
with 'AtteanX::API::JoinRotatingPlanner';

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

sub _join_vars {
	my ($self, $lhs, $rhs) = @_;
	my @vars	= (@{ $lhs->in_scope_variables }, @{ $rhs->in_scope_variables });
	my %vars;
	my %join_vars;
	foreach my $v (@vars) {
		if ($vars{$v}++) {
			$join_vars{$v}++;
		}
	}
	return keys %join_vars;	
}

sub coalesce {
	my $self	= shift;
	my $p		= shift;
	my ($lhs, $rhs)	= @{ $p->children };
	if (($lhs->isa('Attean::Plan::Quad') and $rhs->isa('Attean::Plan::Quad'))) {
		 #&& (scalar $self->_join_vars($lhs, $rhs) > 0)) {
		return AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs, $rhs], distinct => 0);
	}
	return $p;
}

# Gather patterns into larger BGPs
# around 'join_plans' => sub {
# 	my $orig = shift;
# 	my @params = @_;
# 	my $self	= shift;
# 	my $model			= shift;
# 	my $active_graphs	= shift;
# 	my $default_graphs	= shift;
# 	my $lplans			= shift;
# 	my $rplans			= shift;
# 	my @restargs      = @_;
# 	my @plans;
# 	foreach my $lhs (@{ $lplans }) {
# #		warn "\nLeft: " . $lhs->as_string;
# 		foreach my $rhs (@{ $rplans }) {
# #			warn "\n\tRight: " . $rhs->as_string;
# 			my @join_vars = $self->_join_vars($lhs, $rhs);

# 			if ($lhs->isa('Attean::Plan::Table') && ($rhs->isa('Attean::Plan::Table'))) {
# #				push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$rhs], [$lhs], @restargs)); # Most general solution
# 				# Best known solution for now:
# 				if (scalar(@join_vars) > 0) {
# 					return Attean::Plan::HashJoin->new(children => [$lhs, $rhs], join_variables => \@join_vars, distinct => 0, ordered => []);
# 				} else {
# 					return Attean::Plan::NestedLoopJoin->new(children => [$lhs, $rhs], join_variables => \@join_vars, distinct => 0, ordered => []);
# 				}
# 			} elsif ($lhs->isa('Attean::Plan::Table') && ($rhs->isa('Attean::Plan::Quad'))) {
# 				my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$rhs], distinct => 0, ordered => []);
# 				push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [$lhs], @restargs));
# 			} elsif ($rhs->isa('Attean::Plan::Table') && ($lhs->isa('Attean::Plan::Quad'))) {
# 				my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs], distinct => 0, ordered => []);
# 				push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$rhs], [$new_bgp_plan], @restargs));
# 			} elsif ($lhs->isa('Attean::Plan::Quad') &&
# 				 $rhs->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
# 				push(@plans, AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs, @{ $rhs->children || []} ], distinct => 0, ordered => []));
# 			}
# 			elsif ($rhs->isa('Attean::Plan::Quad') &&
# 					 $lhs->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
# 				push(@plans, AtteanX::Store::SPARQL::Plan::BGP->new(children => [$rhs, @{ $lhs->children || []} ], distinct => 0, ordered => []));
# 			}
# 			elsif ($rhs->isa('AtteanX::Store::SPARQL::Plan::BGP') &&
# 					 $lhs->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
# 				push(@plans, AtteanX::Store::SPARQL::Plan::BGP->new(children => [@{ $lhs->children || []} , @{ $rhs->children || []} ], distinct => 0, ordered => []));
# 			}
# 			elsif ($rhs->isa('Attean::Plan::Quad') &&
# 					 $lhs->isa('Attean::Plan::Quad')) {
# 				push(@plans, AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs, $rhs], distinct => 0, ordered => []));
# 			}
# 			elsif ($lhs->isa('Attean::Plan::Quad') && $rhs->does('Attean::API::Plan::Join')) {
# 				if (${$rhs->children}[0]->isa('Attean::Plan::Quad')) {
# 					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs, ${$rhs->children}[0]], distinct => 0, ordered => []);
# 					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [${$rhs->children}[1]], @restargs));
# 				} elsif (${$rhs->children}[1]->isa('Attean::Plan::Quad')) {
# 					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs, ${$rhs->children}[1]], distinct => 0, ordered => []);
# 					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [${$rhs->children}[0]], @restargs));
# 				} elsif (${$rhs->children}[0]->isa('Attean::Plan::Table') && ${$rhs->children}[1]->isa('Attean::Plan::Table')) {
# 					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$lhs], [$rhs], @restargs)); # TODO: Is this correct?
# 				} else {
# 					# Now, deal with any bare quads
# 					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs], distinct => 0, ordered => []);
# 					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [$rhs], @restargs));

# 					#	warn 'Probably a bug! RHS child plans were ' . ref(${$rhs->children}[0]) . ' and ' . ref(${$rhs->children}[1]);
# 				}
# 			}
# 			elsif ($rhs->isa('Attean::Plan::Quad') && $lhs->does('Attean::API::Plan::Join')) {
# 				if (${$lhs->children}[0]->isa('Attean::Plan::Quad')) {
# 					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$rhs, ${$lhs->children}[0]], distinct => 0, ordered => []);
# 					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [${$lhs->children}[1]], @restargs));
# 				} elsif (${$lhs->children}[1]->isa('Attean::Plan::Quad')) {
# 					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$rhs, ${$lhs->children}[1]], distinct => 0, ordered => []);
# 					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [${$lhs->children}[0]], @restargs));
# 				} elsif (${$lhs->children}[0]->isa('Attean::Plan::Table') && ${$lhs->children}[1]->isa('Attean::Plan::Table')) {
# 					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$lhs], [$rhs], @restargs)); # TODO: Is this correct?
# 				} else {
# 					# Now, deal with any bare quads
# 					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$rhs], distinct => 0, ordered => []);
# 					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [$lhs], @restargs));

# 					#	warn 'Probably a bug! LHS child plans were ' . ref(${$lhs->children}[0]) . ' and ' . ref(${$lhs->children}[1]);
# 				}
# 			}

# 		}
# 	}

# 	my $i = 0;
# 	# foreach my $pl (@plans) {
# 	# 	print "Result $i :" . $pl->as_string;
# 	# 	$i++;
# 	# }

# 	unless (@plans) {
# 		@plans = $orig->(@params);
# 	}
# 	return @plans;
# };


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

