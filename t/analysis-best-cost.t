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

This software is copyright (c) 2015, 2016 by Kjetil Kjernsmo.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.


=cut

use v5.14;
use autodie;
use utf8;
use Test::Modern;

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


# my $p	= AtteanX::QueryPlanner::Cache->new;
# isa_ok($p, 'Attean::QueryPlanner');
# isa_ok($p, 'AtteanX::QueryPlanner::Cache');
# does_ok($p, 'Attean::API::CostPlanner');


my $store = Attean->get_store('SPARQL')->new('endpoint_url' => iri('http://test.invalid/'));
my $model = AtteanX::Query::Cache::Analyzer::Model->new(store => $store, cache => $cache);

subtest '3-triple BGP where cache breaks the join to cartesian' => sub {

	my $query = <<'END';
SELECT * WHERE {
  ?a <c> ?s . 
  ?s <p> ?o . 
  ?o <b> "2" .
}
END
	
	$model->cache->set('?v002 <p> ?v001 .', {'<http://example.org/foo>' => ['<http://example.org/bar>'],
														  '<http://example.com/foo>' => ['<http://example.org/baz>', '<http://example.org/foobar>']});
	my $analyzer = AtteanX::Query::Cache::Analyzer->new(model => $model, query => $query, store => $redis1);
	my @patterns = $analyzer->best_cost_improvement;
	is(scalar @patterns, 2, '2 patterns to submit');
	foreach my $pattern (@patterns) {
		isa_ok($pattern, 'Attean::TriplePattern');
		ok($pattern->predicate->compare(iri('p')), 'Predicate is not <p>');
	}
};

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
TODO: {
	local $TODO = 'Depends on the planner';
	is(scalar @patterns, 2, '2 patterns to submit');
}
	foreach my $pattern (@patterns) {
		isa_ok($pattern, 'Attean::TriplePattern');
		ok($pattern->predicate->compare(iri('p')), 'Predicate is not <p>');
		ok($pattern->predicate->compare(iri('r')), 'Predicate is not <r>');

	}
};

done_testing();
