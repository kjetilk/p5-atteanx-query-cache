use 5.010001;
use strict;
use warnings;

package AtteanX::IDPQueryPlanner::TPFCache;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001';

use Moo;

extends 'Attean::IDPQueryPlanner';

#sub access_plans {
	# Checking the cache here
	# Then check TPF and remote SPARQL
#}


1;

__END__

=pod

=encoding utf-8

=head1 NAME

AtteanX::IDPQueryPlanner::TPFCache - Extending the query planner with cache and TPF support

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SEE ALSO

=head1 AUTHOR

Kjetil Kjernsmo E<lt>kjetilk@cpan.orgE<gt>.

=head1 COPYRIGHT AND LICENCE

This software is copyright (c) 2015 by Kjetil Kjernsmo.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.


=head1 DISCLAIMER OF WARRANTIES

THIS PACKAGE IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.

