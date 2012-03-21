#!/usr/bin/perl

print "{ \"points\":[";
open FILE,"20110811_10000lines.txt" or die $!;
while(<FILE>){
    #print $_;
    if($_ =~ /(http:\/\/.+\D)\s+/){
        #print $_;
        chomp($_);
        @vals = split(/\s+/, $_);
        ##print scalar(@vals);
        #print "\"".$vals[1]."\", ";
        print $vals[1]."\n";
    }
}
close FILE;
print "]}";
