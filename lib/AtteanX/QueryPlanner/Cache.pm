use 5.010001;
use strict;
use warnings;


package AtteanX::QueryPlanner::Cache;
use Class::Method::Modifiers;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001';

use Moo;
use Types::Standard qw(InstanceOf);
use Attean::RDF qw(triplepattern variable iri);
use Carp;
use AtteanX::Store::SPARQL::Plan::BGP;

extends 'Attean::QueryPlanner';
with 'Attean::API::NaiveJoinPlanner', 'Attean::API::SimpleCostPlanner';

with 'AtteanX::API::JoinRotatingPlanner', 'MooX::Log::Any';

with 'AtteanX::Query::AccessPlan::Cache';

# Only allow rotation on joins who have one child matching:
# - Either a Attean::Plan::Quad or AtteanX::Store::SPARQL::Plan::BGP
# and the other child being a join
# 
sub allow_join_rotation {
	my $self	= shift;
	my $join	= shift;
	my $quads	= 0;
	my $joins	= 0;
	my @grandchildren;
 	$self->log->trace("Seeking to rotate:\n" . $join->as_string);
	foreach my $p (@{ $join->children }) {
		$quads++ if ($p->isa('Attean::Plan::Quad'));
		$quads++ if ($p->isa('AtteanX::Store::SPARQL::Plan::BGP'));
		if ($p->does('Attean::API::Plan::Join')) {
			$joins++;
			push(@grandchildren, @{ $p->children });
		}
	}
	return 0 unless ($joins == 1);
	return 0 unless ($quads == 1);
	foreach my $p (@grandchildren) {
		$quads++ if ($p->isa('Attean::Plan::Quad'));
		$quads++ if ($p->isa('AtteanX::Store::SPARQL::Plan::BGP'));
	}
	
	if ($quads >= 2) {
		$self->log->debug("Allowing rotation for $quads quads.");
		return 1;
	} else {
 		$self->log->debug("Disallowing rotation, just $quads quad.");
		return 0;
	}
}

sub coalesce_rotated_join {
	my $self	= shift;
	my $p		= shift;
	my @quads;
	my ($lhs, $rhs)	= @{ $p->children };
	my @join_vars	= $self->_join_vars($lhs, $rhs);
	if (scalar(@join_vars)) {
		foreach my $q ($lhs, $rhs) {
			if ($q->isa('Attean::Plan::Quad')) {
				push(@quads, $q);
			} elsif ($q->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
				push(@quads, @{ $q->children });
			} else {
				return $p; # bail-out
			}
		}
		
		my $count	= scalar(@quads);
		my $c	= AtteanX::Store::SPARQL::Plan::BGP->new(children => \@quads, distinct => 0);
		if ($self->log->is_debug && $count >= 3) {
		 	$self->log->debug("Coalescing $lhs and $rhs into BGP with $count quads");
		 	$self->log->trace($c->as_string);
		}
		return $c;
	}
	return $p;
}

# Gather patterns into larger BGPs
around 'join_plans' => sub {
	my $orig = shift;
	my @params = @_;
	my $self	= shift;
	my $model			= shift;
	my $active_graphs	= shift;
	my $default_graphs	= shift;
	my $lplans			= shift;
	my $rplans			= shift;
	my @restargs      = @_;
	my @plans;
	foreach my $lhs (@{ $lplans }) {
		$self->log->trace("BGP Constructing Left:\n" . $lhs->as_string);
		foreach my $rhs (@{ $rplans }) {
			$self->log->trace("BGP Constructing Right:\n" . $rhs->as_string);
			my @join_vars = $self->_join_vars($lhs, $rhs);

			if ($lhs->isa('Attean::Plan::Table') && ($rhs->isa('Attean::Plan::Table'))) {
#				push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$rhs], [$lhs], @restargs)); # Most general solution
				# Best known solution for now:
				if (scalar(@join_vars) > 0) {
					return Attean::Plan::HashJoin->new(children => [$lhs, $rhs], join_variables => \@join_vars, distinct => 0, ordered => []);
				} else {
					return Attean::Plan::NestedLoopJoin->new(children => [$lhs, $rhs], join_variables => \@join_vars, distinct => 0, ordered => []);
				}
			} elsif ($lhs->isa('Attean::Plan::Table') && ($rhs->isa('Attean::Plan::Quad'))) {
				my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$rhs], distinct => 0, ordered => []);
				push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [$lhs], @restargs));
				my $new_ldf_plan = AtteanX::Store::LDF::Plan::Triple->new(subject => $rhs->subject, predicate => $rhs->predicate, object => $rhs->object, distinct => 0);
 				push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_ldf_plan], [$lhs], @restargs));
			} elsif ($rhs->isa('Attean::Plan::Table') && ($lhs->isa('Attean::Plan::Quad'))) {
				my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs], distinct => 0, ordered => []);
				push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$rhs], [$new_bgp_plan], @restargs));
				my $new_ldf_plan = AtteanX::Store::LDF::Plan::Triple->new(subject => $lhs->subject, predicate => $lhs->predicate, object => $lhs->object, distinct => 0);
 				push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_ldf_plan], [$rhs], @restargs));
			} elsif ($lhs->isa('Attean::Plan::Quad') &&
				 $rhs->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
				 if (scalar(@join_vars)) {
					push(@plans, AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs, @{ $rhs->children || []} ], distinct => 0, ordered => []));
				} else {
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs], distinct => 0, ordered => []);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [$rhs], @restargs));
					my $new_ldf_plan = AtteanX::Store::LDF::Plan::Triple->new(subject => $lhs->subject, predicate => $lhs->predicate, object => $lhs->object, distinct => 0);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_ldf_plan], [$rhs], @restargs));
				}
			}
			elsif ($rhs->isa('Attean::Plan::Quad') &&
					 $lhs->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
				if (scalar(@join_vars)) {
					push(@plans, AtteanX::Store::SPARQL::Plan::BGP->new(children => [$rhs, @{ $lhs->children || []} ], distinct => 0, ordered => []));
				} else {
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$rhs], distinct => 0, ordered => []);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$lhs], [$new_bgp_plan], @restargs));
					my $new_ldf_plan = AtteanX::Store::LDF::Plan::Triple->new(subject => $rhs->subject, predicate => $rhs->predicate, object => $rhs->object, distinct => 0);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_ldf_plan], [$lhs], @restargs));
				}
			}
			elsif ($rhs->isa('AtteanX::Store::SPARQL::Plan::BGP') &&
					 $lhs->isa('AtteanX::Store::SPARQL::Plan::BGP')) {
				if (scalar(@join_vars)) {
					push(@plans, AtteanX::Store::SPARQL::Plan::BGP->new(children => [@{ $lhs->children || []} , @{ $rhs->children || []} ], distinct => 0, ordered => []));
				} else {
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$lhs], [$rhs], @restargs));
				}
			}
			elsif ($rhs->isa('Attean::Plan::Quad') &&
					 $lhs->isa('Attean::Plan::Quad')) {
				if (scalar(@join_vars)) {
					push(@plans, AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs, $rhs], distinct => 0, ordered => []));
				} else {
					my $lhs_bgp = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs], distinct => 0, ordered => []);
					my $rhs_bgp = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$rhs], distinct => 0, ordered => []);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$lhs_bgp], [$rhs_bgp], @restargs));
					my $lhs_ldf = AtteanX::Store::LDF::Plan::Triple->new(subject => $lhs->subject, predicate => $lhs->predicate, object => $lhs->object, distinct => 0);
					my $rhs_ldf = AtteanX::Store::LDF::Plan::Triple->new(subject => $rhs->subject, predicate => $rhs->predicate, object => $rhs->object, distinct => 0);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$lhs_ldf], [$rhs_ldf], @restargs));
				}
			} elsif ($lhs->isa('Attean::Plan::Quad') && $rhs->does('Attean::API::Plan::Join')) {
				my ($lhs_child, $rhs_child)	= @{ $rhs->children };
				my @left_join_vars = $self->_join_vars($lhs, $lhs_child);
				my @right_join_vars = $self->_join_vars($lhs, $rhs_child);
				if ($lhs_child->isa('Attean::Plan::Quad') and scalar(@left_join_vars)) {
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs, $lhs_child], distinct => 0, ordered => []);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [$rhs_child], @restargs));
				} elsif ($rhs_child->isa('Attean::Plan::Quad') and scalar(@right_join_vars)) {
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs, $rhs_child], distinct => 0, ordered => []);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [$lhs_child], @restargs));
				} elsif ($lhs_child->isa('Attean::Plan::Table') && $rhs_child->isa('Attean::Plan::Table')) {
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs], distinct => 0, ordered => []);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [$rhs], @restargs)); # TODO: Is this correct?
					my $new_ldf_plan = AtteanX::Store::LDF::Plan::Triple->new(subject => $lhs->subject, predicate => $lhs->predicate, object => $lhs->object, distinct => 0);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_ldf_plan], [$rhs], @restargs));
				} else {
					# Now, deal with any bare quads
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$lhs], distinct => 0, ordered => []);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [$rhs], @restargs));
					my $new_ldf_plan = AtteanX::Store::LDF::Plan::Triple->new(subject => $lhs->subject, predicate => $lhs->predicate, object => $lhs->object, distinct => 0);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_ldf_plan], [$rhs], @restargs));
					$self->log->debug('RHS child plans are ' . ref($lhs_child) . ' and ' . ref($rhs_child));
				}
			}
			elsif ($rhs->isa('Attean::Plan::Quad') && $lhs->does('Attean::API::Plan::Join')) {
				my ($lhs_child, $rhs_child)	= @{ $lhs->children };
				my @left_join_vars = $self->_join_vars($rhs, $lhs_child);
				my @right_join_vars = $self->_join_vars($rhs, $rhs_child);
				if ($lhs_child->isa('Attean::Plan::Quad') and scalar(@left_join_vars)) {
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$rhs, $lhs_child], distinct => 0, ordered => []);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [$rhs_child], @restargs));
				} elsif ($rhs_child->isa('Attean::Plan::Quad') and scalar(@right_join_vars)) {
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$rhs, $rhs_child], distinct => 0, ordered => []);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [$lhs_child], @restargs));
				} elsif ($lhs_child->isa('Attean::Plan::Table') && $rhs_child->isa('Attean::Plan::Table')) {
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$rhs], distinct => 0, ordered => []);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$lhs], [$new_bgp_plan], @restargs)); # TODO: Is this correct?
					my $new_ldf_plan = AtteanX::Store::LDF::Plan::Triple->new(subject => $rhs->subject, predicate => $rhs->predicate, object => $rhs->object, distinct => 0);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_ldf_plan], [$lhs], @restargs));
				} else {
					# Now, deal with any bare quads
					my $new_bgp_plan = AtteanX::Store::SPARQL::Plan::BGP->new(children => [$rhs], distinct => 0, ordered => []);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_bgp_plan], [$lhs], @restargs));
					my $new_ldf_plan = AtteanX::Store::LDF::Plan::Triple->new(subject => $rhs->subject, predicate => $rhs->predicate, object => $rhs->object, distinct => 0);
					push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$new_ldf_plan], [$lhs], @restargs));
					$self->log->debug('LHS child plans are ' . ref($lhs_child) . ' and ' . ref($rhs_child));
				}
			} else {
				push(@plans, $orig->($self, $model, $active_graphs, $default_graphs, [$lhs], [$rhs], @restargs));
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

AtteanX::QueryPlanner::Cache - Extending the query planner with cache and SPARQL support

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SEE ALSO

=head1 AUTHOR

Kjetil Kjernsmo E<lt>kjetilk@cpan.orgE<gt>.

=head1 COPYRIGHT AND LICENCE

This software is copyright (c) 2015, 2016 by Kjetil Kjernsmo.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.


=head1 DISCLAIMER OF WARRANTIES

THIS PACKAGE IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.

