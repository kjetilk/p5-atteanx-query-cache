=pod

=encoding utf-8

=head1 PURPOSE

Test around the post-execution analysis.

=head1 SYNOPSIS

It may come in handy to enable logging for debugging purposes, e.g.:

  LOG_ADAPTER=Screen DEBUG=1 prove -lv t/analysis.t

This requires that L<Log::Any::Adapter::Screen> is installed.

=head1 AUTHOR

Kjetil Kjernsmo E<lt>kjetilk@cpan.orgE<gt>.

=head1 COPYRIGHT AND LICENCE

This software is copyright (c) 2015 by Kjetil Kjernsmo.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.


=cut

use v5.14;
use autodie;
use utf8;
use Test::Modern;
use Digest::SHA qw(sha1_hex);
use CHI;
#use Carp::Always;
use Redis;
use Test::RedisServer;
use Attean;
use Attean::RDF;
use AtteanX::Query::Cache::Analyzer;
use Data::Dumper;
use AtteanX::Model::SPARQLCache;
use Log::Any::Adapter;
Log::Any::Adapter->set($ENV{LOG_ADAPTER} || 'Stderr') if ($ENV{TEST_VERBOSE});

my $cache = CHI->new( driver => 'Memory', global => 1 );

my $redis_server;
eval {
	$redis_server = Test::RedisServer->new;
} or plan skip_all => 'redis-server is required to this test';

my $redis1 = Redis->new( $redis_server->connect_info );

is $redis1->ping, 'PONG', 'Redis Pubsub ping pong ok';


my $basequery =<<'EOQ';
PREFIX dbo: <http://dbpedia.org/ontology/> 
CONSTRUCT {
  ?place a dbo:PopulatedPlace .
  ?place dbo:populationTotal ?pop .
} WHERE {
  ?place a dbo:PopulatedPlace .
  ?place dbo:populationTotal ?pop .
  FILTER (?pop < 50)
}
EOQ

my $store = Attean->get_store('SPARQL')->new('endpoint_url' => iri('http://test.invalid/'));
my $model = AtteanX::Query::Cache::Analyzer::Model->new(store => $store, cache => $cache);
my $analyzer1 = AtteanX::Query::Cache::Analyzer->new(model => $model, query => $basequery, store => $redis1);

my @patterns = $analyzer1->count_patterns;
is(scalar @patterns, 0, 'Nothing now');


$basequery =~ s/< 50/> 5000000/;

my $analyzer2 = AtteanX::Query::Cache::Analyzer->new(model => $model, query => $basequery, store => $redis1);

my @patterns = $analyzer2->count_patterns;
is(scalar @patterns, 0, 'Still nothing');


my $analyzer3 = AtteanX::Query::Cache::Analyzer->new(model => $model, query => $basequery, store => $redis1);


$basequery =~ s/a dbo:PopulatedPlace/dbo:abstract ?abs/g;


my @patterns = $analyzer3->count_patterns;
is(scalar @patterns, 1, 'Two patterns');



done_testing();
