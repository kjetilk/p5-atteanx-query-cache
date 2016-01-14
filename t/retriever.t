=pod

=encoding utf-8

=head1 PURPOSE

Test that produced plans are correct.

=head1 SYNOPSIS

It may come in handy to enable logging for debugging purposes, e.g.:

  LOG_ADAPTER=Screen DEBUG=1 prove -lv t/idp_sparql_planner.t

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
use Attean::RDF qw(triple triplepattern variable iri literal);

use AtteanX::Query::Cache::Retriever;
use AtteanX::Model::SPARQLCache;
use Carp::Always;
use Log::Any::Adapter;
Log::Any::Adapter->set($ENV{LOG_ADAPTER} || 'Stderr') if ($ENV{TEST_VERBOSE});


package TestCreateStore {
	use Moo;
	with 'Test::Attean::Store::SPARQL::Role::CreateStore';
};

my $triples = [
				   triple(iri('http://example.org/foo'), iri('http://example.org/p'), literal('1')),
				   triple(iri('http://example.org/bar'), iri('http://example.org/p'), literal('1')),
				   triple(iri('http://example.com/foo'), iri('http://example.org/p'), literal('dahut')),
				   triple(iri('http://example.com/bar'), iri('http://example.org/http://dahut/p'), iri('http://example.org/dahutten')),
				   triple(iri('http://example.org/dahut'), iri('http://example.org/dahut'), literal('1')),
				  ];


my $test = TestCreateStore->new;
my $store = $test->create_store(triples => $triples);
my $model = AtteanX::Model::SPARQLCache->new(store => $store, 
															cache => CHI->new( driver => 'Memory', 
																					 global => 1 ));

my $retriever = AtteanX::Query::Cache::Retriever->new(model => $model);

subtest 'Simple single-variable triple' => sub {
	my $t = triplepattern(variable('s'), iri('http://example.org/p'), literal('1'));
	my $data = $retriever->fetch($t);
	is(ref($data), 'ARRAY', 'We have arrayref');
	is_deeply($data, ['<http://example.org/bar> .','<http://example.org/foo> .'], 'expected arrayref');
};




done_testing;
