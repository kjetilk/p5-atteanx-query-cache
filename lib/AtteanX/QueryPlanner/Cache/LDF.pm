use 5.010001;
use strict;
use warnings;


package AtteanX::QueryPlanner::Cache::LDF;
use Class::Method::Modifiers;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001_01';

use Moo;
use AtteanX::Query::AccessPlan::LDF;

extends 'AtteanX::QueryPlanner::Cache';
with 'AtteanX::Query::AccessPlan::LDF';

before 'allow_join_rotation' => sub {
	$_[2] //= 0;
	my @grandchildren;
 	$_[0]->log->trace("Seeking to rotate LDFs in:\n" . $_[1]->as_string);
	foreach my $p (@{ $_[1]->children }) {
		$_[2]++ if ($p->isa('AtteanX::Store::LDF::Plan::Triple'));
	}

	foreach my $p (@grandchildren) {
		$_[2]++ if ($p->isa('AtteanX::Store::LDF::Plan::Triple'));
	}
};

1;
