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
use Carp::Always;

use Attean;
use Attean::RDF;
use AtteanX::Query::Cache::Analyzer;
use Data::Dumper;
use AtteanX::Model::SPARQLCache;
use Log::Any::Adapter;
Log::Any::Adapter->set($ENV{LOG_ADAPTER} || 'Stderr') if ($ENV{TEST_VERBOSE});

my $cache = CHI->new( driver => 'Memory', global => 1 );

# my $p	= AtteanX::QueryPlanner::Cache->new;
# isa_ok($p, 'Attean::QueryPlanner');
# isa_ok($p, 'AtteanX::QueryPlanner::Cache');
# does_ok($p, 'Attean::API::CostPlanner');



my $store = Attean->get_store('SPARQL')->new('endpoint_url' => iri('http://test.invalid/'));
my $model = AtteanX::Query::Cache::Analyzer::Model->new(store => $store, cache => $cache);
my $analyzer = AtteanX::Query::Cache::Analyzer->new(model => $model, query => 'SELECT * WHERE { ?s a ?o; <p> <dahut> }');

$analyzer->analyze;
