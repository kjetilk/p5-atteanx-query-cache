=pod

=encoding utf-8

=head1 PURPOSE

Test that we can fetch and cache

=head1 SYNOPSIS

It may come in handy to enable logging for debugging purposes, e.g.:

  LOG_ADAPTER=Screen DEBUG=1 prove -lv t/analysis.t

This requires that L<Log::Any::Adapter::Screen> is installed.

=head1 AUTHOR

Kjetil Kjernsmo E<lt>kjetilk@cpan.orgE<gt>.

=head1 COPYRIGHT AND LICENCE

This software is copyright (c) 2015, 2016 by Kjetil Kjernsmo.

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
use AtteanX::Query::Cache::Retriever;
use Log::Any::Adapter;
Log::Any::Adapter->set($ENV{LOG_ADAPTER} || 'Stderr') if ($ENV{TEST_VERBOSE});

package TestCreateStore {
	use Moo;
	with 'Test::Attean::Store::SPARQL::Role::CreateStore';
};

my $triples = [
				   triple(iri('http://example.org/bar'), iri('http://example.org/c'), iri('http://example.org/foo')),
				   triple(iri('http://example.org/foo'), iri('http://example.org/p'), iri('http://example.org/baz')),
				   triple(iri('http://example.org/baz'), iri('http://example.org/b'), literal('2')),
				   triple(iri('http://example.com/foo'), iri('http://example.org/p'), literal('dahut')),
				   triple(iri('http://example.org/dahut'), iri('http://example.org/dahut'), literal('1')),
				  ];


my $test = TestCreateStore->new;
my $store = $test->create_store(triples => $triples);
my $model = AtteanX::Query::Cache::Analyzer::Model->new(store => $store, 
																		  cache => CHI->new( driver => 'Memory', 
																									global => 1 ));

my $retriever = AtteanX::Query::Cache::Retriever->new(model => $model);

my $redis_server;
eval {
	$redis_server = Test::RedisServer->new;
} or plan skip_all => 'redis-server is required to this test';

my $redis1 = Redis->new( $redis_server->connect_info );

is $redis1->ping, 'PONG', 'Redis Pubsub ping pong ok';


note '3-triple BGP where cache breaks the join to cartesian';

my $query = <<'END';
SELECT * WHERE {
  ?a <http://example.org/c> ?s . 
  ?s <http://example.org/p> ?o . 
  ?o <http://example.org/b> "2" .
}
END

$model->cache->set('?v002 <p> ?v001 .', {'<http://example.org/foo>' => ['<http://example.org/bar>'],
													  '<http://example.com/foo>' => ['<http://example.org/baz>', '<http://example.org/foobar>']});
my $analyzer = AtteanX::Query::Cache::Analyzer->new(model => $model, query => $query, store => $redis1);
my $count = $analyzer->analyze_and_cache('best_cost_improvement');
is($count, 2, 'Two triple patterns has match');


done_testing;
exit 0;
note 'This test is CPU intensive';
subtest '4-triple BGP where one pattern makes little impact' => sub {

my $query = <<'END';
SELECT * WHERE {
	?s <r> "1" .
   ?s <p> ?o .
	?s <q> "xyz" . 
	?o <b> <c> . 
}
END

	my $analyzer = AtteanX::Query::Cache::Analyzer->new(model => $model, query => $query, store => $redis1);
	my @patterns = $analyzer->best_cost_improvement;
	is(scalar @patterns, 2, '2 patterns to submit');
	foreach my $pattern (@patterns) {
		isa_ok($pattern, 'Attean::TriplePattern');
		ok($pattern->predicate->compare(iri('p')), 'Predicate is not <p>');
		ok($pattern->predicate->compare(iri('r')), 'Predicate is not <r>');

	}
};

done_testing();
