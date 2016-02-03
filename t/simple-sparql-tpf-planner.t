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

use CHI;

use Attean;
use Attean::RDF;
use AtteanX::QueryPlanner::Cache::LDF;
use AtteanX::Store::Memory;
#use Carp::Always;
use Data::Dumper;
use AtteanX::Store::SPARQL;
use AtteanX::Store::LDF;
use AtteanX::Model::SPARQLCache::LDF;
use Log::Any::Adapter;
Log::Any::Adapter->set($ENV{LOG_ADAPTER} || 'Stderr', category => qr/AtteanX::QueryPlanner::Cache AtteanX::Model::SPARQLCache::LDF/) if ($ENV{TEST_VERBOSE});

my $cache = CHI->new( driver => 'Memory', global => 1 );

my $p	= AtteanX::QueryPlanner::Cache::LDF->new;
isa_ok($p, 'Attean::QueryPlanner');
isa_ok($p, 'AtteanX::QueryPlanner::Cache::LDF');
does_ok($p, 'Attean::API::CostPlanner');

package TestLDFCreateStore {
        use Moo;
        with 'Test::Attean::Store::LDF::Role::CreateStore';
};

my $test = TestLDFCreateStore->new;


{

	my $sparqlstore	= Attean->get_store('SPARQL')->new('endpoint_url' => iri('http://test.invalid/sparql'));
	isa_ok($sparqlstore, 'AtteanX::Store::SPARQL');

	my $graph = iri('http://test.invalid/graph');
	my $t		= triple(variable('s'), iri('http://example.org/m/p'), literal('1'));
	my $u		= triple(variable('s'), iri('http://example.org/m/p'), variable('o'));
	my $v		= triple(variable('s'), iri('http://example.org/m/q'), blank('xyz'));
	my $w		= triple(variable('a'), iri('http://example.org/m/b'), iri('http://example.org/m/c'));
	my $x		= triple(variable('s'), iri('http://example.org/m/q'), iri('http://example.org/m/a'));
	my $y		= triple(variable('o'), iri('http://example.org/m/b'), literal('2'));
	my $z		= triple(variable('a'), iri('http://example.org/m/c'), variable('s'));
	my $s		= triple(iri('http://example.org/m/a'), variable('p'), variable('o'));

	my $ldfstore	= $test->create_store(triples => [$t,$u,$v,$w,$x,$y,$z,$s]);

	isa_ok($ldfstore, 'AtteanX::Store::LDF');
	my $model	= AtteanX::Model::SPARQLCache::LDF->new( store => $sparqlstore,
																		  ldf_store => $ldfstore,
																		  cache => $cache );
	isa_ok($model, 'AtteanX::Model::SPARQLCache::LDF');
	isa_ok($model, 'AtteanX::Model::SPARQLCache');
	isa_ok($model, 'AtteanX::Model::SPARQL');

	subtest 'Empty BGP, to test basics' => sub {
		#plan skip_all => 'it works';
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
		$cache->set('?v001 <http://example.org/m/p> "1" .', ['<http://example.org/foo>', '<http://example.org/bar>']);
		$cache->set('?v001 <http://example.org/m/p> "dahut" .', ['<http://example.com/foo>', '<http://example.com/bar>']);
		$cache->set('?v001 <http://example.org/m/dahut> "1" .', ['<http://example.org/dahut>']);
		#plan skip_all => 'it works';
		
		ok($model->is_cached(triplepattern(variable('foo'), iri('http://example.org/m/p'), literal('1'))->canonicalize->tuples_string), 'Cache has been set');
		ok(! $model->is_cached(triplepattern(variable('foo'), iri('http://example.org/m/q'), literal('1'))->canonicalize->tuples_string), 'Cache has not been set');
		my $bgp		= Attean::Algebra::BGP->new(triples => [$u]);
		my @plans = $p->plans_for_algebra($bgp, $model, [$graph]);
		is(scalar @plans, 2, 'Two plans');
		my $plan = shift @plans;
		does_ok($plan, 'Attean::API::Plan', '1-triple BGP');
		isa_ok($plan, 'AtteanX::Store::LDF::Plan::Triple');
		is($plan->plan_as_string, 'LDFTriple { ?s, <http://example.org/m/p>, ?o }', 'Good LDF plan');
		is($model->cost_for_plan($plan), 381, 'Cost for plan is 381');
		$plan = shift @plans;
		does_ok($plan, 'Attean::API::Plan', '1-triple BGP');
		isa_ok($plan, 'Attean::Plan::Quad');
		is($plan->plan_as_string, 'Quad { ?s, <http://example.org/m/p>, ?o, <http://test.invalid/graph> }', 'Good plan');
	};

done_testing;
exit 0;

	subtest '1-triple BGP two variables, with cache' => sub {
		note("A 1-triple BGP should produce a single Attean::Plan::Table plan object");
		$cache->set('?v002 <http://example.org/m/p> ?v001 .', {'<http://example.org/foo>' => ['<http://example.org/bar>'],
													 '<http://example.com/foo>' => ['<http://example.org/baz>', '<http://example.org/foobar>']});
		$cache->set('?v001 <http://example.org/m/p> "dahut" .', ['<http://example.com/foo>', '<http://example.com/bar>']);
		$cache->set('?v002 <http://example.org/m/dahut> ?v001 .', {'<http://example.org/dahut>' => ['"Foobar"']});
		#plan skip_all => 'it works';

		ok($model->is_cached(triplepattern(variable('foo'), iri('http://example.org/m/p'), variable('bar'))->canonicalize->tuples_string), 'Cache has been set');
		my $bgp		= Attean::Algebra::BGP->new(triples => [$u]);

		my @plans = $p->plans_for_algebra($bgp, $model, [$graph]);
		is(scalar @plans, 3, "Got three plans");
		my $plan = $plans[0];
		does_ok($plan, 'Attean::API::Plan', '1-triple BGP');
		isa_ok($plan, 'Attean::Plan::Table');
		does_ok($plans[1], 'Attean::API::Plan', '1-triple BGP');
		isa_ok($plans[1], 'AtteanX::Store::LDF::Plan::Triple');
		is($plans[1]->plan_as_string, 'LDFTriple { ?s, <http://example.org/m/p>, ?o }', 'Good plan');
		does_ok($plans[2], 'Attean::API::Plan', '1-triple BGP');
		isa_ok($plans[2], 'Attean::Plan::Quad');
		is($plans[2]->plan_as_string, 'Quad { ?s, <http://example.org/m/p>, ?o, <http://test.invalid/graph> }', 'Good plan');
	};


	subtest '2-triple BGP with join variable with cache on both' => sub {
		#plan skip_all => 'it works';
		note("A 2-triple BGP with a join variable and without any ordering should produce two tables joined, no LDF interfering");
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t, $u]);
		my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);
		is(scalar @plans, 1, 'Got just 1 plans');
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
		#plan skip_all => 'it works';
		my $bgp		= Attean::Algebra::BGP->new(triples => [$w, $z]);
		my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);
		is(scalar @plans, 5, 'Got 5 plans');
		my $plan = shift @plans;
		isa_ok($plan, 'AtteanX::Store::SPARQL::Plan::BGP', 'First Plan is SPARQLBGP');
		like($plan->as_string, qr/SPARQLBGP/, 'SPARQL BGP serialisation');
		foreach my $cplan (@{$plan->children}) {
			does_ok($cplan, 'Attean::API::Plan', 'Each child of 2-triple BGP');
			isa_ok($cplan, 'Attean::Plan::Quad', 'Child is a Quad');
			ok(! $cplan->isa('AtteanX::Store::LDF::Plan::Triple'), 'But not an LDF triple');
		}
		foreach my $plan (@plans) {
			does_ok($plan, 'Attean::API::Plan::Join', 'The rest are joins');
			foreach my $cplan (@{$plan->children}) {
				does_ok($cplan, 'Attean::API::Plan', 'Each child of 2-triple BGP is a plan');
				isa_ok($cplan, 'AtteanX::Store::LDF::Plan::Triple', 'which is a LDF triple');
			}
		}
	};


	subtest '2-triple BGP with join variable with cache one cached' => sub {
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t, $x]);
		my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);
		is(scalar @plans, 5, 'Got 5 plans');
		foreach my $plan (@plans){
			print "\n". $plan->as_string;
		}
		# The first two plans should be the "best", containing a HashJoin over
		# a Table and a LDF.
		foreach my $plan (@plans[0..1]) {
			does_ok($plan, 'Attean::API::Plan::Join', 'First 2 plans are joins');
			my @tables	= $plan->subpatterns_of_type('Attean::Plan::Table');
			is(scalar(@tables), 1, 'First 2 plans contain 1 table sub-plan');
			my @ldfs	= $plan->subpatterns_of_type('AtteanX::Store::LDF::Plan::Triple');
			is(scalar(@ldfs), 1, 'First 2 plans contain 1 table sub-plan');
		}

		my $plan = $plans[0];
		
		# sorting the strings should result in the Attean::Plan::Table being first
		my @children	= sort { "$a" cmp "$b" } @{$plan->children};
		foreach my $cplan (@children) {
			does_ok($cplan, 'Attean::API::Plan', 'Each child of 2-triple BGP');
		}
		
		my ($table,$ldfplan)	= @children;
		isa_ok($table, 'Attean::Plan::Table', 'Should join on Table first');
		isa_ok($ldfplan, 'AtteanX::Store::LDF::Plan::Triple', 'Then on LDF triple');
		is($ldfplan->plan_as_string, 'LDFTriple { ?s, <http://example.org/m/q>, <http://example.org/m/a> }', 'Child plan OK');
	};
	done_testing;
	exit 0;


	subtest '4-triple BGP with join variable with cache one cached, no LDFs' => sub {
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t, $u, $y, $x]);
		my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);
		foreach my $plan (@plans) {
			warn $plan->as_string;
		}
		is(scalar @plans, 5, 'Got 5 plans');
		my $plan = $plans[0];
		does_ok($plan, 'Attean::API::Plan::Join');
		my @c1plans = sort @{$plan->children};
		isa_ok($c1plans[0], 'Attean::Plan::Table', 'First child when sorted is a table');
		isa_ok($c1plans[1], 'AtteanX::Store::SPARQL::Plan::BGP', 'Second child when sorted is a BGP');
		is(scalar @{$c1plans[1]->children}, 3, '...with three children');
		foreach my $plan (@{$c1plans[1]->children}) {
			isa_ok($plan, 'Attean::Plan::Quad', 'All children are quads');
		}
	};

	done_testing;
	exit 0;
	



	subtest '5-triple BGP with join variable with cache two cached' => sub {
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t, $u, $v, $w, $x]);
		my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);
		is(scalar @plans, 5, 'Got 5 plans');
		my $plan = $plans[0];
		does_ok($plan, 'Attean::API::Plan::Join');
		my @c1plans = sort @{$plan->children};
		does_ok($c1plans[0], 'Attean::API::Plan::Join', 'First child when sorted is a join');
		does_ok($c1plans[1], 'AtteanX::Store::SPARQL::Plan::BGP', 'Second child when sorted is a BGP');
		is(scalar @{$c1plans[1]->children}, 2, '...with two quads');
		my @c2plans = sort @{$c1plans[0]->children};
		isa_ok($c2plans[0], 'Attean::Plan::HashJoin', 'First grandchild when sorted is a hash join');
	 	foreach my $cplan (@{$c2plans[0]->children}) {
			isa_ok($cplan, 'Attean::Plan::Table', 'and children of them are tables');
		}
		isa_ok($c2plans[1], 'AtteanX::Store::SPARQL::Plan::BGP', 'Second grandchild when sorted is a BGP');
		is(scalar @{$c2plans[1]->children}, 1, '...with one quad');
	};

	subtest '3-triple BGP where cache breaks the join to cartesian' => sub {
		my $bgp		= Attean::Algebra::BGP->new(triples => [$z, $u, $y]);
		my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);
		is(scalar @plans, 5, 'Got 5 plans');
		my $plan = shift @plans;
		isa_ok($plan, 'AtteanX::Store::SPARQL::Plan::BGP', 'The winning plan should be BGP');
		is(scalar @{$plan->children}, 3, 'with three children');
		$plan = shift @plans;
		isa_ok($plan, 'Attean::Plan::HashJoin', 'The next plan should be a join');
		
		# sorting the strings should result in a HashJoin followed by a SPARQLBGP
		my @children	= sort { "$a" cmp "$b" } @{ $plan->children };
		foreach my $plan (@children) {
			does_ok($plans[0], 'Attean::API::Plan');
		}
		
		my @triples;
		my ($join, $bgpplan1)	= @children;
		isa_ok($join, 'Attean::Plan::HashJoin');
		isa_ok($bgpplan1, 'AtteanX::Store::SPARQL::Plan::BGP');
		is(scalar(@{ $bgpplan1->children }), 1);
		push(@triples, @{ $bgpplan1->children });
		
		# sorting the strings should result in a Table followed by a SPARQLBGP
		my @grandchildren	= sort { "$a" cmp "$b" } @{ $join->children };
		foreach my $plan (@grandchildren) {
			does_ok($plans[0], 'Attean::API::Plan');
		}
		my ($table, $bgpplan2)	= @grandchildren;
		isa_ok($table, 'Attean::Plan::Table');
		isa_ok($bgpplan2, 'AtteanX::Store::SPARQL::Plan::BGP');
		is(scalar(@{ $bgpplan2->children }), 1);
		push(@triples, @{ $bgpplan2->children });
		my @strings	= sort map { $_->as_string } @triples;
		my @expected	= (
			qq[- Quad { ?a, <http://example.org/m/c>, ?s, <http://test.invalid/graph> }\n],
			qq[- Quad { ?o, <http://example.org/m/b>, "2", <http://test.invalid/graph> }\n],
		);
		is_deeply(\@strings, \@expected);
	};

	subtest '3-triple BGP chain with cache on two' => sub {
		# TODO: Also improve with cost model
		$cache->set('?v001 <b> "2" .', ['<http://example.com/dahut>']);
		my $bgp		= Attean::Algebra::BGP->new(triples => [$z, $u, $y]);
		my @plans	= $p->plans_for_algebra($bgp, $model, [$graph]);
		my $plan = shift @plans;
		does_ok($plan, 'Attean::API::Plan::Join');
		my @tables	= $plan->subpatterns_of_type('Attean::Plan::Table');
		is(scalar @tables, 2, 'Should be 2 tables in the plan');
		my @bgps	= $plan->subpatterns_of_type('AtteanX::Store::SPARQL::Plan::BGP');
		is(scalar @bgps, 1, 'Should be only one BGP in the plan');
		is(scalar @{ $bgps[0]->children }, 1, 'And that should just have one child');
		isa_ok(${ $bgps[0]->children }[0], 'Attean::Plan::Quad', 'That has a Quad child');
	};


	subtest '3-triple BGP with predicate variable' => sub {
		$cache->set('<http://example.org/m/a> ?v002 ?v001 .', {'<http://example.org/m/p>' => ['<http://example.org/bar>'],
													 '<http://example.org/m/q>' => ['<http://example.org/baz>', '<http://example.org/foobar>']});
		my $bgp		= Attean::Algebra::BGP->new(triples => [$s, $u, $y]);
		my $plan	= $p->plan_for_algebra($bgp, $model, [$graph]);
		does_ok($plan, 'Attean::API::Plan::Join');
		isa_ok($plan, 'Attean::Plan::HashJoin');
		my @cplans = sort @{$plan->children};
		isa_ok($cplans[0], 'Attean::Plan::HashJoin', 'First child is hashjoin');
		foreach my $c2plan (@{$cplans[0]->children}) {
			isa_ok($c2plan, 'Attean::Plan::Table', 'and children of them are tables');
		}
		isa_ok($cplans[1], 'Attean::Plan::Table', 'Other child is a table');

	};
}

done_testing();
