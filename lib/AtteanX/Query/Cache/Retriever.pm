package AtteanX::Query::Cache::Retriever;

use 5.010001;
use strict;
use warnings;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001';

use Moo;
use AtteanX::Store::SPARQL::Plan::BGP;
use Carp qw(croak);
use Attean::RDF;
use Types::Standard qw(InstanceOf);

has model => (is => 'ro',
				  isa => InstanceOf['AtteanX::Model::SPARQLCache'],
				  handles => [ qw(cache) ],
				  required => 1);

with 'MooX::Log::Any';


sub fetch {
	my ($self, $triple) = @_;
	$triple = $triple->canonicalize;
	my $key = $triple->tuples_string;
	my $use_hash = (scalar $triple->values_consuming_role('Attean::API::Variable')) - 1;
	if ($use_hash < 0) {
		croak "No variables in triple pattern $key";
	} elsif ($use_hash > 1) {
		croak "Only triple patterns with one or two variables are supported, got $key";
	}
	my $sparql = "SELECT * WHERE {\n\t" . $triple->as_sparql . '. }';
	$self->log->debug("Running SPARQL query\n$sparql");
	my $iter = $self->model->get_sparql($sparql);

	if ($use_hash) { # Now, decide if we insert an array or a hash into the cache.
		my $data;
		while (my $res = $iter->next) {
#			warn Data::Dumper::Dumper($res->values);
			my @values = $res->values;
			push(@{$data->{$values[0]->ntriples_string}}, $values[1]->ntriples_string);
		}
		return $data;
	} else {
		my @data;
		while (my $res = $iter->next) {
			my ($value) = $res->values;
			push(@data, $value->ntriples_string);
		}
		return \@data;
	}
}



1;
