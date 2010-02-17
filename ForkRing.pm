package ForkRing;
# ForkRing attempts to provide a constant interface
# to a revolving door of potentially failing processes
use Moose;

# code is the code that get passed to forkbabies
has code        => ( is=>'rw', isa => 'CodeRef' );
has currentBaby => ( is=>'rw', isa => 'ForkBaby' );

sub runTests {
    TestForkRing::run();
}

sub getNewBaby {
    my ($self) = @_;
    my $sub = $self->code();
    my $baby = ForkBaby->new(code => $sub);
    $baby->forkit(); #start the baby!
    $self->currentBaby($baby);
    return $baby;
}

#this will make a new baby is necessary
sub get_baby {
    my ($self) = @_;
    my $baby = $self->currentBaby();
    if ($baby) {
        return $baby;
    } else {
        return $self->getNewBaby()
    }
}

sub send {
    my ($self,$msg) = @_;
    dwarn("Getting Baby");
    my $baby = $self->get_baby();
    dwarn("Sending baby");
    dwarn("[$$] Sending baby: [$msg] ".$baby->hasForked());
    return $baby->send($msg);
}
sub dwarn {}
1;
package ForkBaby;
#Fork Baby wraps underlying perl code
#The perl code will just read and write
use Moose;
$| = 1;
use IO::Pipe;
use Time::Out qw(timeout) ;
$SIG{PIPE} = 'IGNORE';
use POSIX ":sys_wait_h";
sub REAPER {
    my $waitedpid = wait;
    # loathe sysV: it makes us not only reinstate
    # the handler, but place it after the wait
    $SIG{CHLD} = \&REAPER;
}
$SIG{CHLD} = \&REAPER;




has isParent          => ( is => 'rw', isa => 'Int', default => 0); #Pid
has hasForked         => ( is => 'rw', isa => 'Int', default => 0); #Pid
has fromParentToChild => ( is => 'rw', isa => 'IO::Pipe');
has fromChildToParent => ( is => 'rw', isa => 'IO::Pipe');
has code              => ( is => 'rw', isa => 'CodeRef' );
has timeoutSeconds    => ( is => 'rw', isa => 'Int', default => 10); #timeout time

sub iopipe {
    my $pipe = IO::Pipe->new();
    return $pipe;
}


sub send {
    my ($self,$msg) = @_;
    dwarn("Checking baby");
    my $hasForked = $self->hasForked();
    my $isParent = $self->isParent();
    die "[$$] [send] Not a parent, not forked [$msg] [$hasForked] [$isParent]" unless ($hasForked && $isParent);
    die "Not a parent, not forked [$msg]" unless (my $child_pid = $self->isParent());
    die "Command has a newline in it!" if ($msg =~ m#$/#); 
    my $writer = $self->fromParentToChild();
    dwarn("Writing to baby!");
    print $writer ($msg.$/);
    # assume writer is flushed..
    return $self->readResponse();
}

sub childPid {
    my ($self) = @_;
    $self->isParent();
}

sub forkit {
    my ($self) = @_;
    my $p2c = iopipe();
    my $c2p = iopipe();
    $self->fromChildToParent($c2p);
    $self->fromParentToChild($p2c);
    my $pid;
    if ($pid = fork()) {
        dwarn("[$$] Has parent now");
        $self->isParent($pid);
        $self->hasForked(1);
        $p2c->writer();
        $p2c->autoflush(1);
        $c2p->reader();
        $c2p->autoflush(1);
    } else {
        $self->isParent(0); # child
        $p2c->reader();
        $c2p->writer();
        $c2p->autoflush(1);
        $p2c->autoflush(1);
        return $self->childLoop();
    }    
}

sub readFromParent {
    my ($self) = @_;
    my $read_pipe = $self->fromParentToChild();
    my $line = <$read_pipe>;
    return $line;
}
sub childLoop {
    my ($self) = @_;
    dwarn("Child reading from parent!");
    while (my $command = $self->readFromParent()) {
        dwarn("Have command! $command");
        my ($msg) = $self->parseCommand($command);
        my $res = $self->evalMsg($msg);
        # chances are we're a different process now! isn't that neat ;)
    }
    exit(0); #we're done yeah!
}

sub evalMsg {
    my ($self, $msg) = @_;
    my $result_pipe = iopipe();
    my $pid;
    my $results = undef;
    if ($pid = fork()) {
        $result_pipe->reader();
        $result_pipe->autoflush(1);
        my $time = $self->timeoutSeconds();
        my $SUCCESS = undef;
        timeout $time => sub {
            $SUCCESS = <$result_pipe>;
            chomp($SUCCESS) if $SUCCESS;
            my $chars = <$result_pipe>;
            #dwarn("Read: $chars");
            $chars = _how_many_chars($chars);
            #dwarn("CHARS: $chars");
            $results="";
            my $n = $result_pipe->read($results, $chars);
            die "Read $n not $chars chars!" unless $n == $chars;
            my $nl = <$result_pipe>;
            ($results = "", $SUCCESS="failure") unless defined($nl) && ($nl eq $/);
            close($result_pipe);
            $result_pipe = undef;
        };
        if ($@ || $SUCCESS ne "SUCCESS"){
            # timeout occurred :(
            # kill the child
            # send error up
            # stay alive
            if (kill 0 => $pid) {
                kill 9 => $pid;
            }
            $self->sendFailure( results => $results );
        } else {
            # send info to parent
            $self->sendSuccess(pid => $pid, results => $results);
            # kill self
            exit(0);
        }
    } else {
        # in the child
        $result_pipe->writer();
        $result_pipe->autoflush(1);
        # we're a child lets run this command
        my $res = $self->runMsg($msg);
        # haha! we're still alive right!
        print $result_pipe "SUCCESS$/";
        print $result_pipe "CHARS: ".length($res).$/;
        print $result_pipe $res;
        print $result_pipe $/;
        close($result_pipe);
        $result_pipe = undef;
    }    
    

}
sub cleanUpChildren {
    my ($self) = @_;
    my $pid = $self->isParent();
    if (kill 0 => $pid) {
        kill 9 => $pid;
    }
}
sub parseCommand {
    my ($self,$command) = @_;
    chomp($command);
    return $command;
}


sub runMsg {
    my ($self,$msg) = @_;
    my $sub = $self->code();
    my $res = &$sub($self,$msg);
    return $res;
}


# Is there something better?
sub sendSuccess {
    my ($self,%hash) = @_;
    my $pid = $hash{pid};
    my $str = $hash{results};
    my $len = length($str)||0;
    my $write_fd = $self->fromChildToParent();
    print $write_fd "SUCCESS$/";
    print $write_fd "NEW PID: $pid$/";
    print $write_fd "CHARS: $len$/"; #plus 1 newline!
    print $write_fd $str;
    print $write_fd $/;
}
sub sendFailure {
    my ($self,%hash) = @_;
    my $str = $hash{results};
    my $len = length($str)||0;
    my $write_fd = $self->fromChildToParent();
    print $write_fd "FAILURE$/";
    print $write_fd "CHARS: $len$/"; #plus 1 newline!
    print $write_fd $str;
    print $write_fd $/;
}

sub readResponse {
    my ($self) = @_;
    dwarn("Reading response");
    my $hasForked = $self->hasForked();
    my $isParent = $self->isParent();
    die "Not a parent, not forked [$hasForked] [$isParent]" unless ($hasForked && $isParent);
    my $reader = $self->fromChildToParent();
    my $success_or_failure = <$reader>;
    chomp($success_or_failure) if defined $success_or_failure;
    #dwarn("Result: $success_or_failure") if defined($success_or_failure);
    if (!defined($success_or_failure)) {
        warn "unexplained failure [probably death]";
        $self->cleanUpChildren();
        $self->forkit();
        return {type => "FAILURE",
                failure => 1,
                results => "Not sure",
               };
    } elsif ($success_or_failure eq 'SUCCESS') {
        my $pid = <$reader>;
        dwarn("Read: $pid");
        $pid = _parse_pid($pid);
        dwarn("PID: $pid");
        my $chars = <$reader>;
        dwarn("Read: $chars");
        $chars = _how_many_chars($chars);
        dwarn("CHARS: $chars");
        my $results="";
        my $n = $reader->read($results, $chars);
        die "Read $n not $chars chars!" unless $n == $chars;
        my $nl = <$reader>;
        die "[S] Not a newline: [$nl] [$results]" unless $nl eq $/;
        return {type => "SUCCESS",
                success => 1, 
                pid => $pid,
                len => $chars,
                results => $results,
               };
    } elsif ($success_or_failure eq 'FAILURE') {
        my $chars = <$reader>;
        $chars = _how_many_chars($chars);
        my $results="";
        my $n = $reader->read($results, $chars);
        die "Read $n not $chars chars!" unless $n == $chars;
        my $nl = <$reader>;
        die "[F] Not a newline: [$nl] [$results]" unless $nl eq $/;
        return {type => "FAILURE",
                failure => 1, 
                len => $chars,
                results => $results
               };
    } else { #like an undef
        die "Unexplained input $success_or_failure";
    }
}
sub _how_many_chars {
    my ($str) = @_;
    chomp($str);
    my ($chars) = ($str =~ /CHARS:\s+(\d+)$/);
    return $chars || 0;
}
sub _parse_pid {
    my ($str) = @_;
    chomp($str);
    my ($chars) = ($str =~ /NEW PID:\s+(\d+)$/);
    return $chars || 0;
}
sub dwarn {}
1;
package TestForkRing;
use strict;
use Data::Dumper;
sub proof_of_death {
    open(my $pod ,">>","proof_of_death.log");
    print $pod (@_,$/);
    close($pod);
    warn @_;
    die @_;
}

sub run {
    my $run = sub {
        my ($self,$data) = @_;
        srand();
        if (int(rand(10))==0) {
            proof_of_death "Random Death [$data]";
        } else {
            return localtime();
        }
    };
    my $fr = ForkRing->new( code => $run );
    for my $i (1..100) {
        #warn("Test $_");
        my $res = undef;
        do {
            $res = $fr->send("Test $i");
            warn "Not successful $i" if !$res->{success};
        } until ($res->{success})
    }
    warn "We finally go to 100!";
    my $response = sub {
        my ($self,$data) = @_;
        srand();

        if (int(rand(10))==0) {
            proof_of_death "Response Random Death [$data]";
        } else {
            return lc($data);
        }
    };
    $fr = undef;
    $fr = ForkRing->new( code => $response );
    for my $c ('A'..'Z') {
        my $res;
        do {
            $res = $fr->send($c);
            warn "Not successful $c" if !$res->{success};
        } until ($res->{success});
        my $resc = $res->{results};
        chomp($resc);
        lc($c) eq $resc or  die "not LC'd? ".Dumper($res);
    }
    warn "We got from A..Z";

    my $state = 0;
    my $state_test = sub {
        my ($self,$data) = @_;
        warn "ST: $data $state";
        srand();

        if (int(rand(10))==0) {
            proof_of_death "State Test Random Death [$data]";
        } else {
            warn "Didn't die $data $state";
            $state += $data;
            return $state;
        }
    };
    my $cmpstate = 0;
    $fr = ForkRing->new( code => $state_test );
    for my $c (1..100) {
        $cmpstate += $c;
        my $res;
        do {
            $res = $fr->send($c);
            warn "Not successful $c" if !$res->{success};
        } until ($res->{success});
        my $resc = $res->{results};
        #warn "$cmpstate $resc";
        $cmpstate == $resc or  die "not State != State [$cmpstate] [$resc] ".Dumper($res);

    }
    warn "We got from 1..100";
    warn "Good everything worked!";
}
