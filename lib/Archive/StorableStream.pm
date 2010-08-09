package Archive::StorableStream;

use strict;
use warnings;

our $VERSION = v0.1.0;

#Signature
our $sign="StorStrm000100"; 

# Don't change the signature lenght or you will brake
# Version compatibility. Keep the signature with 14 chars.
our $signlen=14;
our $signvstart=8;
our $signvlen=$signlen-$signvstart;
# Just to make sure the signature was not broken.
die "Invalid Signature\n"
	unless length($sign) == $signlen
		and substr($sign, $signvstart, $signvlen) =~ m{^\d+$};

my $ioblocksize=1024;

use Carp qw(carp croak);
use Storable qw(nfreeze thaw);
use IO::Handle;
use IO::File;
use bytes;

sub new {
	my $class = shift;
	my $self = bless {}, $class;

	my %args;
	if ($#_ == 0) {
		if (ref $_[0] or defined fileno $_[0]) {
			$args{fh} = shift;
		} else {
			$args{filename} = shift;
		}
	} else {
		%args=@_;
	}

	$self->init(%args);

	return $self;
}

sub init {
	my $self=shift;
	my %args=@_;
	my $mode = $args{'mode'} || '+<';

	if ($args{fh}) {
		if (ref $args{fh}) {
			$self->{_fh} = $args{fh};
		} else {
			my $io=IO::Handle->new();
			if ($io->fdopen(fileno($args{fh}), $mode)) {
				$self->{_fh} = $io;
			} else {
				carp "Can't open the handle: $!";
			}
		}
	} elsif ($args{filename}) {
		if (my $io = IO::File->new($args{filename}, $mode)) {
			$self->{_fh} = $io;
		} else {
			carp "Can't open the file: $!";
		}
	} else {
		carp ref($self)." needs a fh or filename argument";
	}

	$self->{_signed} = 0;
	$self->{__buf} = '';

	delete $args{fh};
	delete $args{filename};
	delete $args{mode};

	$self->{flags}->{compress} = 0;

	$self->{args} = \%args;
}

sub put {
	my $self=shift;

	unless ($self->{_signed}) {
		$self->{_fh}->print($sign, "\0");

		my $flags = "";
		$flags .= 'C' if $self->{flags}->{compress};
		$self->{_fh}->print(length($flags)."\0".$flags);
		$self->{_signed} = 1;
	}

	while (my $ref = shift) {
		my $frozen=nfreeze($ref);
		$self->{_fh}->print(length($frozen)."\0".$frozen);
	}
}

sub get {
	my $self=shift;

	unless ($self->{_signed}) {
		my $sig;
		$self->{_fh}->read($sig, $signlen);
		if (substr($sig,0,$signvstart) eq substr($sign,0,$signvstart)) {
			unless (substr($sig, $signvstart, $signvlen)
					<= substr($sig, $signvstart, $signvlen)) {
				carp("Trying to read a bogen file from a newer version\n".
					  "\tLib version: ".substr($sign, $sign, $signvstart)."\n".
					  "\tFile version: ".substr($sig, $signvstart, $signvlen)."\n");
			}
			$self->_read();
			if ($self->{__buf}=~m{^(\d+)\0}) {
				my $fs = $1; # blockSize
				substr($self->{__buf}, 0, length($fs) + 1) = '';
				my $flags=substr($self->{__buf}, 0, $fs);
				substr($self->{__buf}, 0, $fs) = '';
				$self->{flags}->{compress}=1 if $flags=~s/C//g;
			} else {
				carp("Trying to read a invalid file");
			}
			$self->{_signed}=1;
		} else {
			carp "Not a StorableStream file: Missing signature";
		}
	}

	return if $self->{_fh}->eof() and length($self->{__buf})==0;

	$self->_read() unless $self->{__buf}=~m{^(\d+)\0};
	return if $self->{_fh}->eof() and length($self->{__buf})==0;

	if ($self->{__buf}=~m{^(\d+)\0}) {
		my $bs=$1; # blockSize
		substr($self->{__buf}, 0, length($bs) + 1) = '';
		$self->_read() while length($self->{__buf}) < $bs;

		my $block=substr($self->{__buf},0,$bs);
		substr($self->{__buf},0,$bs)='';

		return thaw($block);
	} else {
		carp "OhOohh: StorableStream file with unexpected format\n";
	}
}

sub _read {
	my $self=shift;

	return if $self->{_fh}->eof();

	my $buf='';
	$self->{_fh}->read($buf, $ioblocksize);

	$self->{__buf} .= $buf;
}

1;
