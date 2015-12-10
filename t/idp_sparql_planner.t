use v5.14;
use autodie;
use utf8;
use Test::Modern;
use Digest::SHA qw(sha1_hex);
use CHI;

use Attean;
use Attean::RDF;
use AtteanX::IDPQueryPlanner::Cache;
use AtteanX::Store::Memory;
#use Carp::Always;
use Data::Dumper;
use AtteanX::Store::SPARQL;
use AtteanX::Model::SPARQLCache;

my $cache = CHI->new( driver => 'Memory', global => 1 );

my $p	= AtteanX::IDPQueryPlanner::Cache->new;
isa_ok($p, 'Attean::IDPQueryPlanner');
isa_ok($p, 'AtteanX::IDPQueryPlanner::Cache');
does_ok($p, 'Attean::API::CostPlanner');

# TODO: add data to the cache
# for two bound: An array of variable
# For one bound: A hash (or two hashes?)
# Dictionary?

{

	my $store	= Attean->get_store('SPARQL')->new('endpoint_url' => iri('http://test.invalid/'));
	isa_ok($store, 'AtteanX::Store::SPARQL');
	my $model	= AtteanX::Model::SPARQLCache->new( store => $store, cache => $cache );
	my $graph = iri('http://test.invalid/graph');
	my $t		= triple(variable('s'), iri('p'), literal('1'));
	my $u		= triple(variable('s'), iri('p'), variable('o'));
	my $v		= triple(variable('s'), iri('q'), blank('xyz'));
	my $w		= triple(variable('a'), iri('b'), iri('c'));
	my $x		= triple(variable('s'), iri('q'), iri('a'));
	my $z		= triple(variable('a'), iri('b'), variable('s'));
	my $y		= triple(variable('o'), iri('b'), literal('2'));

	subtest 'Empty BGP, to test basics' => sub {
		note("An empty BGP should produce the join identity table plan");
		my $bgp		= Attean::Algebra::BGP->new(triples => []);
		my $plan	= $p->plan_for_algebra($bgp, $model, [$graph]);
		does_ok($plan, 'Attean::API::Plan', 'Empty BGP');
		isa_ok($plan, 'Attean::Plan::Table');
		my $rows	= $plan->rows;
		is(scalar(@$rows), 1);
	};


	subtest '1-triple BGP single variable, with cache, not cached' => sub {
		note("A 1-triple BGP should produce a single Attean::Plan::Table plan object");
		$cache->set('?subject <p> "1" .', ['<http://example.org/foo>', '<http://example.org/bar>']);
		$cache->set('?subject <p> "dahut" .', ['<http://example.com/foo>', '<http://example.com/bar>']);
		$cache->set('?subject <dahut> "1" .', ['<http://example.org/dahut>']);
		
		my $bgp		= Attean::Algebra::BGP->new(triples => [$u]);
		my $plan	= $p->plan_for_algebra($bgp, $model, [$graph]);
		does_ok($plan, 'Attean::API::Plan', '1-triple BGP');
		isa_ok($plan, 'Attean::Plan::Quad');
		is($plan->plan_as_string, 'Quad { ?s, <p>, ?o, <http://test.invalid/graph> }', 'Good plan');
	};

	subtest '1-triple BGP two variables, with cache' => sub {
		note("A 1-triple BGP should produce a single Attean::Plan::Table plan object");
		$cache->set('?subject <p> ?object .', {'<http://example.org/foo>' => ['<http://example.org/bar>'],
															'<http://example.com/foo>' => ['<http://example.org/baz>', '<http://example.org/foobar>']});
		$cache->set('?subject <p> "dahut" .', ['<http://example.com/foo>', '<http://example.com/bar>']);
		$cache->set('?subject <dahut> ?object .', {'<http://example.org/dahut>' => ['"Foobar"']});
		my $bgp		= Attean::Algebra::BGP->new(triples => [$u]);

		my @plans = $p->plans_for_algebra($bgp, $model, [$graph]);
		is(scalar @plans, 2, "Got two plans");
		my $plan = $plans[0];
		does_ok($plan, 'Attean::API::Plan', '1-triple BGP');
		isa_ok($plan, 'Attean::Plan::Table');
		my $rows	= $plan->rows;
		is(scalar(@$rows), 3, 'Got three rows back');
		foreach my $row (@$rows) {
			my @vars = sort $row->variables;
			is(scalar(@vars), 2, 'Each result has two variables');
			is($vars[0], 'o', 'First variable name is correct');
			is($vars[1], 's', 'Second variable name is correct');
			does_ok($row->value('s'), 'Attean::API::IRI');
			does_ok($row->value('o'), 'Attean::API::IRI');
		}
		my @testrows = sort {$a->value('o')->as_string cmp $b->value('o')->as_string} @$rows;

		ok($testrows[0]->value('s')->equals(iri('http://example.org/foo')), 'First triple subject IRI is OK'); 
		ok($testrows[0]->value('o')->equals(iri('http://example.org/bar')), 'First triple object IRI is OK'); 
		ok($testrows[1]->value('s')->equals(iri('http://example.com/foo')), 'Second triple subject IRI is OK'); 
		ok($testrows[1]->value('o')->equals(iri('http://example.org/baz')), 'Second triple object IRI is OK'); 
		ok($testrows[2]->value('s')->equals(iri('http://example.com/foo')), 'Third triple subject IRI is OK'); 
		ok($testrows[2]->value('o')->equals(iri('http://example.org/foobar')), 'Third triple object IRI is OK'); 

		does_ok($plans[1], 'Attean::API::Plan', '1-triple BGP');
		isa_ok($plans[1], 'Attean::Plan::Quad');
		is($plans[1]->plan_as_string, 'Quad { ?s, <p>, ?o, <http://test.invalid/graph> }', 'Good plan');
	};

	subtest '1-triple BGP single variable object, with cache' => sub {
		note("A 1-triple BGP should produce a single Attean::Plan::Table plan object");
		$cache->set('<http://example.org/foo> <p> ?object .', ['<http://example.org/foo>', '<http://example.org/bar>']);
		$cache->set('<http://example.org/foo> <dahut> ?object .', ['"Le Dahu"@fr', '"Dahut"@en']);
		$cache->set('?subject <dahut> "Dahutten"@no .', ['<http://example.org/dahut>']);
		my $tp = triplepattern(iri('http://example.org/foo'),
									  iri('dahut'),
									  variable('name'));
		my $bgp		= Attean::Algebra::BGP->new(triples => [$tp]);
		my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);
		is(scalar @plans, 2, 'Got two plans');
		my $plan = $plans[0];
		does_ok($plan, 'Attean::API::Plan', '1-triple BGP');
		isa_ok($plan, 'Attean::Plan::Table');
		my $rows	= $plan->rows;
		is(scalar(@$rows), 2, 'Got two rows back');
		foreach my $row (@$rows) {
			my @vars = $row->variables;
			is($vars[0], 'name', 'Variable name is correct');
			does_ok($row->value('name'), 'Attean::API::Literal');
		}
		ok(${$rows}[0]->value('name')->equals(langliteral('Le Dahu', 'fr')), 'First literal is OK'); 
		ok(${$rows}[1]->value('name')->equals(langliteral('Dahut', 'en')), 'Second literal is OK'); 

		does_ok($plans[1], 'Attean::API::Plan', '1-triple BGP');
		isa_ok($plans[1], 'Attean::Plan::Quad');
		is($plans[1]->plan_as_string, 'Quad { <http://example.org/foo>, <dahut>, ?name, <http://test.invalid/graph> }', 'Good plan');
	};

	subtest '2-triple BGP with join variable with cache on both' => sub {
		note("A 2-triple BGP with a join variable and without any ordering should produce two tables joined");
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t, $u]);
		my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);
		is(scalar @plans, 2, 'Got just 2 plans');
		foreach my $plan (@plans) {
#			warn $plan->as_string;
			does_ok($plan, 'Attean::API::Plan::Join', 'Plans are join plans');
			ok($plan->distinct, 'Plans should be distinct');
			foreach my $cplan (@{$plan->children}) {
				does_ok($cplan, 'Attean::API::Plan', 'Each child of 2-triple BGP');
				isa_ok($cplan, 'Attean::Plan::Table', 'All children should be Table');
			}
		}
		my $plan = $plans[0];
		isa_ok($plan, 'Attean::Plan::HashJoin', '2-triple BGP with Tables should return HashJoin');
	};

	subtest '2-triple BGP with join variable with cache none cached' => sub {
		my $bgp		= Attean::Algebra::BGP->new(triples => [$w, $x]);
		my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);
		is(scalar @plans, 2, 'Got two plans');
		foreach my $plan (@plans) {
			isa_ok($plan, 'AtteanX::Store::SPARQL::Plan::BGP', 'Plans are SPARQLBGP');
		}
		my $plan = $plans[0];
		does_ok($plan, 'Attean::API::Plan', '2-triple BGP');
		like($plan->as_string, qr/SPARQLBGP/, 'SPARQL BGP serialisation');
		foreach my $cplan (@{$plan->children}) {
			does_ok($cplan, 'Attean::API::Plan', 'Each child of 2-triple BGP');
			isa_ok($cplan, 'Attean::Plan::Quad', 'Child is a Quad');
		}
	};

	subtest '2-triple BGP with join variable with cache one cached' => sub {
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t, $x]);
		my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);
		is(scalar @plans, 4, 'Got four plans'); # TODO: Two are identical
		my $plan = $plans[0];
		does_ok($plan, 'Attean::API::Plan::Join', '2-triple BGP');
		ok($plan->distinct, 'Distinct OK');
		foreach my $cplan (@{$plan->children}) {
			does_ok($cplan, 'Attean::API::Plan', 'Each child of 2-triple BGP');
		}
		# TODO: What will the real join order be:
		isa_ok(${$plan->children}[0], 'Attean::Plan::Table', 'Should join on Table first');
		my $bgpplan = ${$plan->children}[1];
		isa_ok($bgpplan, 'AtteanX::Store::SPARQL::Plan::BGP', 'Then on SPARQL BGP');
		isa_ok(${$bgpplan->children}[0], 'Attean::Plan::Quad', 'That has a Quad child');
		is(${$bgpplan->children}[0]->plan_as_string, 'Quad { ?s, <q>, <a>, <http://test.invalid/graph> }', 'Child plan OK');
	};

	subtest '5-triple BGP with join variable with cache two cached' => sub {
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t, $u, $v, $w, $x]);
		my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);
		foreach my $plan (@plans) {
			warn $plan->as_string . "\n";
		}
		my $plan = $plans[0];
		does_ok($plan, 'Attean::API::Plan::Join');
		foreach my $cplan (@{$plan->children}) {
#			warn $cplan->as_string;
			does_ok($cplan, 'Attean::API::Plan', 'Each child of 2-triple BGP');
		}
		# TODO: What will the real join order be:
		isa_ok(${$plan->children}[0], 'Attean::Plan::Quad');
		is(${$plan->children}[0]->plan_as_string, 'Quad { ?s, <q>, <a>, <http://test.invalid/graph> }', 'Child plan OK');
		isa_ok(${$plan->children}[1], 'Attean::Plan::Table');
	};

	subtest '3-triple BGP where cache breaks the join to cartesian' => sub {
		local 
		my $bgp		= Attean::Algebra::BGP->new(triples => [$z, $u, $y]);
		my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);
		does_ok($plans[0], 'Attean::API::Plan::Join');
		foreach my $plan (@plans) {
			warn $plan->as_string . "\n";
		}
	};

}

done_testing();
