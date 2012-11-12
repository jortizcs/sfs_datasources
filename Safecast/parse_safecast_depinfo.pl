#!/usr/bin/perl

@labels = ();

open FILE, "depinfo_t.txt" or die $!;
print "{\n\t\"feeds\":[\n\t\t";
$line=0;
while (<FILE>){
    $line +=1;
    if($line == 2){
        chomp($_);
        @labels = split("\t", $_);
        for($i=0; $i<=17; $i++){
            $labels[$i]=~ s/\R//g;
            #print $labels[$i]."\n";
        }
    }
    if($line >2){
        chomp($_);
        @vals = split("\t", $_);
        chomp($vals[0]);
        if(length($vals[0])>1){
            print "{\n\t\n";
            for($i=0; $i<=17; $i++){
                $vals[$i]=~ s/\R//g;
                if($i==12){
                    print "\t\t\"".$labels[$i]."\":".$vals[$i];
                } else {
                    print "\t\t\"".$labels[$i]."\":\"".$vals[$i]."\"";
                }
                if($i!=17){
                    print ",\n";
                } else {
                    print "\n";
                }
            }
            print "\t},";
        }
    }
}
close FILE;
print "\n]}";
