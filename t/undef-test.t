use v5.14;
use autodie;
use utf8;
use Test::Modern;

use CHI;

use Attean;
use Attean::RDF;
use AtteanX::QueryPlanner::Cache;
#use Carp::Always;
use Data::Dumper;
use AtteanX::Store::SPARQL;
use AtteanX::Model::SPARQLCache;
use Log::Any::Adapter;
Log::Any::Adapter->set('Screen');

my $cache = CHI->new( driver => 'Memory', global => 1 );

my $p	= AtteanX::QueryPlanner::Cache->new;

# These tests does not actually look up anything in a real store, it just simulates
my $store	= Attean->get_store('SPARQL')->new('endpoint_url' => iri('http://test.invalid/'));
my $model	= AtteanX::Model::SPARQLCache->new( store => $store, cache => $cache );
my $graph = iri('http://test.invalid/graph');
my $t		= triplepattern(variable('s'), iri('p'), literal('1'));
my $u		= triplepattern(variable('s'), iri('p'), variable('o'));
my $v		= triplepattern(variable('s'), iri('q'), blank('xyz'));
my $w		= triplepattern(variable('a'), iri('b'), iri('c'));
my $x		= triplepattern(variable('s'), iri('q'), iri('a'));

$cache->set('?v001 <p> "1" .', ['<http://example.org/foo>', '<http://example.org/bar>']);
$cache->set('?v002 <p> ?v001 .', {'<http://example.org/foo>' => ['<http://example.org/bar>'],
											 '<http://example.com/foo>' => ['<http://example.org/baz>', '<http://example.org/foobar>']});

my $bgp		= Attean::Algebra::BGP->new(triples => [$t, $u, $v, $w, $x]);
$DB::single =1;
my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);

ok(@plans);
