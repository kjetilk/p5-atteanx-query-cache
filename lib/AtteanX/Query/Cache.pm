use 5.010001;
use strict;
use warnings;

package AtteanX::Query::Cache;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001_03';
use Moo;

extends 'AtteanX::Endpoint';



package Plack::App::AtteanX::Query::Cache;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001_03';

use Attean;
use Attean::RDF;
use RDF::Trine;
use Moo;
use AtteanX::Endpoint;
use CHI;
use Redis;
use LWP::UserAgent::CHICaching;
use AtteanX::Model::SPARQLCache::LDF;
use AtteanX::QueryPlanner::Cache::LDF;
use Try::Tiny;

extends 'Plack::App::AtteanX::Endpoint';
with 'MooX::Log::Any';

sub prepare_app {
	my $self = shift;
	my $config = $self->{config};
	my $cache = CHI->new( driver => 'Memory', global => 1 );
	my $sparqlurl = 'http://dbpedia.org/sparql';
	my $ldfurl = 'http://fragments.dbpedia.org/2015/en';
	my $redisserver = 'robin.kjernsmo.net:6379';
	my $sparqlstore = Attean->get_store('SPARQL')->new(endpoint_url => $sparqlurl);
	my $ldfstore    = Attean->get_store('LDF')->new(start_url => $ldfurl);
	my $redissub = Redis->new(server => $redisserver, name => 'subscriber');

	RDF::Trine::default_useragent(LWP::UserAgent::CHICaching->new(cache => $cache));

	my $model	= AtteanX::Model::SPARQLCache::LDF->new( store => $sparqlstore,
																		  ldf_store => $ldfstore,
																		  cache => $cache,
																		  publisher => $redissub);
	$self->{config} = {};

#	try {
	$self->{endpoint} = Attean::Endpoint->new(model => $model,
															planner => AtteanX::QueryPlanner::Cache::LDF->new,
															conf => $self->{config},
															graph => iri('http://example.org/graph'));
	#	  };
#	if ($@) {
#		$self->log->error($@);
#	}
}

1;

__END__

=pod

=encoding utf-8

=head1 NAME

AtteanX::Query::Cache - Experimental prefetching SPARQL query cacher

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 BUGS

Please report any bugs to
L<http://rt.cpan.org/Dist/Display.html?Queue=AtteanX-Query-Cache>.

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

