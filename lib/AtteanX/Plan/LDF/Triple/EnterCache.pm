package AtteanX::Plan::LDF::Triple::EnterCache;
use v5.14;
use warnings;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION = '0.01';

use Moo;
use Class::Method::Modifiers;
use Attean;
use Carp;
use namespace::clean;

extends 'AtteanX::Plan::LDF::Triple';

around 'impl' => sub {
	my $orig = shift;
	my @params = @_;
	my $self	= shift;
	my $model	= shift;
	$model->pubsub->publish('prefetch.triplepattern', $self->tuples_string);
	return $orig->(@params);
};

1;
