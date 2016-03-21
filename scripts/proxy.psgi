#!/usr/bin/env perl

use strict;
use warnings;


use Plack::Request;
use Plack::Builder;
use AtteanX::Query::Cache;
use LWP::MediaTypes qw(add_type);
use RDF::Trine;

add_type( 'application/rdf+xml' => qw(rdf xrdf rdfx) );
add_type( 'text/turtle' => qw(ttl) );
add_type( 'text/plain' => qw(nt) );
add_type( 'text/x-nquads' => qw(nq) );
add_type( 'text/json' => qw(json) );
add_type( 'text/html' => qw(html xhtml htm) );

my $cacher = AtteanX::Query::Cache->new;



builder {
	enable "AccessLog", format => "combined";
	$cacher->to_app;
};
