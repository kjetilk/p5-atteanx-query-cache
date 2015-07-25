use v5.14;
use autodie;
use utf8;
use Test::More;
use Test::Exception;
use Digest::SHA qw(sha1_hex);
use CHI;

use Attean;
use Attean::RDF;
use AtteanX::IDPQueryPlanner::Cache;
use AtteanX::Store::Memory;
use Carp::Always;

my $cache = CHI->new( driver => 'Memory', global => 1 );

my $p	= AtteanX::IDPQueryPlanner::Cache->new;
isa_ok($p, 'Attean::IDPQueryPlanner');
isa_ok($p, 'AtteanX::IDPQueryPlanner::Cache');
does_ok($p, 'Attean::API::CostPlanner');

package TestModel {
	use Moo;
	use Types::Standard qw(InstanceOf);

	extends 'Attean::MutableQuadModel';

	has 'cache' => (
						 is => 'ro',
						 isa => InstanceOf['CHI::Driver'],
						 required => 1
					);
};

# TODO: add data to the cache
# for two bound: An array of variable
# For one bound: A hash (or two hashes?)
# Dictionary?

{

	my $store	= Attean->get_store('SPARQL')->new('endpoint_url' => iri('http://test.invalid/'));
	isa_ok($store, 'AtteanX::Store::SPARQL');
	my $model	= TestModel->new( store => $store, cache => $cache );
	my $graph = iri('http://test.invalid/graph');
	my $t		= triple(variable('s'), iri('p'), literal('1'));
	my $u		= triple(variable('s'), iri('p'), variable('o'));
	my $v		= triple(variable('s'), iri('q'), blank('xyz'));
	my $w		= triple(variable('a'), iri('b'), iri('c'));

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
		use Data::Dumper;
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
		my $plan	= $p->plan_for_algebra($bgp, $model, [$graph]);
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
		my $plan	= $p->plan_for_algebra($bgp, $model, [$graph]);
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

	};



	done_testing;
exit 0;
	subtest '2-triple BGP without join variable' => sub {
		note("A 2-triple BGP without a join variable should produce a distinct nested loop join");
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t, $w]);
		my $plan	= $p->plan_for_algebra($bgp, $model, [$graph]);
		does_ok($plan, 'Attean::API::Plan', '2-triple BGP');
		isa_ok($plan, 'Attean::Plan::NestedLoopJoin');
		ok($plan->distinct);
	};

	subtest '2-triple BGP with join variable' => sub {
		note("A 2-triple BGP with a join variable and without any ordering should produce a distinct hash join");
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t, $u]);
		my $plan	= $p->plan_for_algebra($bgp, $model, [$graph]);
		does_ok($plan, 'Attean::API::Plan', '2-triple BGP');
		isa_ok($plan, 'Attean::Plan::HashJoin');
		ok($plan->distinct);
	};

	subtest 'Distinct 2-triple BGP with join variable, no blank nodes' => sub {
		note("A 2-triple BGP with a join variable without any blank nodes is necessarily distinct, so a distinct operation should be a no-op, resulting in just a nested loop join");
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t, $u]);
		my $dist	= Attean::Algebra::Distinct->new( children => [$bgp] );
		my $plan	= $p->plan_for_algebra($dist, $model);
		does_ok($plan, 'Attean::API::Plan', 'Distinct 2-triple BGP without blanks');
		isa_ok($plan, 'Attean::Plan::HashJoin');
		ok($plan->distinct);
	};

	subtest 'Distinct 3-triple BGP with join variable and blank nodes' => sub {
		note("A 3-triple BGP with a blank node isn't necessarily distinct, so a distinct operation should result in a HashDistinct plan");
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t, $u, $v]);
		my $dist	= Attean::Algebra::Distinct->new( children => [$bgp] );
		my $plan	= $p->plan_for_algebra($dist, $model);
		does_ok($plan, 'Attean::API::Plan', 'Distinct 3-triple BGP with blanks');
		isa_ok($plan, 'Attean::Plan::HashDistinct');
		ok($plan->distinct);
	};
	
	subtest 'Sorted 1-triple BGP' => sub {
		note("A 1-triple BGP with ASC(?s) sorting should result in a Project(Order(Extend(Quad(....)))) pattern");
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t]);
		my $sorted	= order_algebra_by_variables($bgp, 's');
		my $plan	= $p->plan_for_algebra($sorted, $model);
		does_ok($plan, 'Attean::API::Plan', 'Sorted 1-triple BGP'); # Sorting introduces a 
		isa_ok($plan, 'Attean::Plan::Project');
		ok($plan->distinct, 'Plan is distinct');
		
		my $order	= $plan->ordered;
		is(scalar(@$order), 1, 'Count of ordering comparators');
		my $cmp	= $order->[0];
		ok($cmp->ascending, 'Ordering is ascending');
		my $expr	= $cmp->expression;
		isa_ok($expr, 'Attean::ValueExpression');
		is($expr->value->value, 's');
	};
	
	subtest 'Join planning is equivalent to BGP planning' => sub {
		note("A join between two 1-triple BGPs should result in the same plan as the equivalent 2-triple BGP");
		my $plan1		= $p->plan_for_algebra(Attean::Algebra::BGP->new(triples => [$t, $u]), $model);
		my $bgp1		= Attean::Algebra::BGP->new(triples => [$t]);
		my $bgp2		= Attean::Algebra::BGP->new(triples => [$u]);
		my $join		= Attean::Algebra::Join->new(children => [$bgp1, $bgp2]);
		my $plan2		= $p->plan_for_algebra($join, $model);
		
		does_ok($_, 'Attean::API::Plan') for ($plan1, $plan2);
		isa_ok($_, 'Attean::Plan::HashJoin') for ($plan1, $plan2);
		
		# we don't do a single deep comparison on the plans here, because while they are equivalent plans,
		# BGP planning handles the annotating of the distinct flag on sub-plans differently than the
		# general join planning.
		foreach my $pos (0,1) {
			does_ok($_->children->[$pos], 'Attean::API::Plan') for ($plan1, $plan2);
			isa_ok($_->children->[$pos], 'Attean::Plan::Quad') for ($plan1, $plan2);
			is_deeply([$plan1->children->[$pos]->values], [$plan2->children->[$pos]->values]);
		}
	};
	
	subtest 'Variable Filter' => sub {
		note("FILTER(?o) should result in a EBVFilter(...) pattern");
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t]);
		my $expr	= Attean::ValueExpression->new(value => variable('o'));
		my $filter	= Attean::Algebra::Filter->new(children => [$bgp], expression => $expr);
		my $plan	= $p->plan_for_algebra($filter, $model);
		does_ok($plan, 'Attean::API::Plan', 'Variable filter');
		isa_ok($plan, 'Attean::Plan::EBVFilter');
		is($plan->variable, 'o');
	};
	
	subtest 'Expression Filter' => sub {
		note("FILTER(?s && ?o) should result in a Project(EBVFilter(Extend(...))) pattern");
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t]);
		my $expr1	= Attean::ValueExpression->new(value => variable('s'));
		my $expr2	= Attean::ValueExpression->new(value => variable('o'));
		my $expr	= Attean::BinaryExpression->new( operator => '&&', children => [$expr1, $expr2] );
		my $filter	= Attean::Algebra::Filter->new(children => [$bgp], expression => $expr);
		my $plan	= $p->plan_for_algebra($filter, $model);
		does_ok($plan, 'Attean::API::Plan', 'Expression filter');
		isa_ok($plan, 'Attean::Plan::Project');
		isa_ok($plan->children->[0], 'Attean::Plan::EBVFilter');
		isa_ok($plan->children->[0]->children->[0], 'Attean::Plan::Extend');
	};
	
	subtest 'IRI named graph' => sub {
		note("1-triple BGP restricted to an IRI-named graph should result in a Quad plan");
		my $ng		= iri('http://eample.org/named/');
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t]);
		my $named	= Attean::Algebra::Graph->new(children => [$bgp], graph => $ng);
		my $plan	= $p->plan_for_algebra($named, $model);
		does_ok($plan, 'Attean::API::Plan', 'IRI-named graph');
		isa_ok($plan, 'Attean::Plan::Quad');
	};
	
	subtest 'Variable named graph (model with 0 named graphs)' => sub {
		note("1-triple BGP restricted to a variable-named graph should result in an empty Union plan");
		my $ng		= variable('g');
		my $bgp		= Attean::Algebra::BGP->new(triples => [$t]);
		my $named	= Attean::Algebra::Graph->new(children => [$bgp], graph => $ng);
		my $plan	= $p->plan_for_algebra($named, $model);
		does_ok($plan, 'Attean::API::Plan', 'IRI-named graph');
		isa_ok($plan, 'Attean::Plan::Union');
		is(scalar(@{ $plan->children }), 0);
	};
}

done_testing();



sub order_algebra_by_variables {
	my $algebra	= shift;
	my @vars	= @_;
	my @cmps;
	foreach my $var (@vars) {
		my $expr	= Attean::ValueExpression->new(value => variable($var));
		my $cmp		= Attean::Algebra::Comparator->new(ascending => 1, expression => $expr);
		push(@cmps, $cmp);
	}
	my $sorted	= Attean::Algebra::OrderBy->new( children => [$algebra], comparators => \@cmps );
	return $sorted;
}

sub does_ok {
    my ($class_or_obj, $does, $message) = @_;
    $message ||= "The object does $does";
    ok(eval { $class_or_obj->does($does) }, $message);
}


