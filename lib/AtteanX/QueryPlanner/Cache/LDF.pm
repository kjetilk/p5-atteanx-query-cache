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

1;
