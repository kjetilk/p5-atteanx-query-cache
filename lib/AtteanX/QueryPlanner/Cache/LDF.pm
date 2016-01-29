use 5.010001;
use strict;
use warnings;


package AtteanX::QueryPlanner::Cache::LDF;
use Class::Method::Modifiers;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001';

use Moo;

extends 'AtteanX::QueryPlanner::Cache';
with 'AtteanX::Query::AccessPlan::LDF', 'MooX::Log::Any';

# Wrap the plans_for_algebra to insert LDF plans
around 'plans_for_algebra' => sub {
	my $orig = shift;
	my @params = @_;
	my $self	= shift;
	my $algebra	= shift;
	my $model = shift;
	my $active_graphs	= shift;
	my $default_graphs = shift;
	my %args	= @_;
	my @plans = $orig->(@params);
	my $w = Attean::TreeRewriter->new();
	$w->register_pre_handler(sub {
										 my ($tree, $parent_node, $thunk) = @_;
										 print ref($tree) ."\n";
										 my $handled = 0;
										 my $descend = 1;
										 my $rewritten;
										 if ($tree->isa('AtteanX::Store::SPARQL::Plan::BGP') && (scalar @{$tree->children} == 1)) {
											 my $pattern = shift @{$tree->children};
											 $handled = 1;
											 $descend = 0;
											 $rewritten = AtteanX::Store::LDF::Plan::Triple->new(subject => $pattern->subject,
																												  predicate => $pattern->predicate,
																												  object => $pattern->object,
																												  distinct => 0);
										 }
										 if ($handled && $self->log->is_debug) {
											 $self->log->debug("Rewriter will add subplan:\n" . $rewritten->as_string);
										 }
										 return ($handled, $descend, $rewritten);
									 });

	foreach my $plan (@plans) {
		warn $plan->as_string;
		my ($rewritten, $newplan) = $w->rewrite($plan, {});
		if ($rewritten) {
			warn "FOOOOOOOOOOOO " . $newplan->as_string;
		}
	}
};


1;
