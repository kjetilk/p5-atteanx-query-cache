package AtteanX::Plan::TriplePatternFragment 0.005 {
	use Moo;
	use Types::Standard qw(ConsumerOf ArrayRef);
	use RDF::LDF;

	has 'subject'	=> (is => 'ro', required => 1);
	has 'predicate'	=> (is => 'ro', required => 1);
	has 'object'	=> (is => 'ro', required => 1);

	with 'Attean::API::Plan', 'Attean::API::NullaryQueryTree';
	with 'Attean::API::TriplePattern';

	sub plan_as_string {
		my $self	= shift;
		my @nodes	= $self->values;
		my @strings;
		foreach my $t (@nodes) {
			if (ref($t) eq 'ARRAY') {
				my @tstrings	= map { $_->ntriples_string } @$t;
				if (scalar(@tstrings) == 1) {
					push(@strings, @tstrings);
				} else {
					push(@strings, '[' . join(', ', @tstrings) . ']');
				}
			} elsif ($t->does('Attean::API::TermOrVariable')) {
				push(@strings, $t->ntriples_string);
			} else {
				use Data::Dumper;
				die "Unrecognized node in triple pattern: " . Dumper($t);
			}
		}
		return sprintf('TPF { %s }', join(', ', @strings));
	}
	
	sub impl {
		my $self	= shift;
		my $model	= shift;
		my @values	= $self->values;
		# TODO: Where do I get the URL to the fragment from, the model?
		return sub { # TODO: Interface to RDF::LDF
			return $model->get_bindings( @values );
		}
	}
}


1;
