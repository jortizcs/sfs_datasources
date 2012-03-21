#!/usr/bin/perl

open FILE, "mapping.csv" or die $!;
print "{\n";
$line=0;
while (<FILE>){
    $line +=1;
    if($line != 1){
        chomp($_);
        @vals = split(",", $_);
        $size = $#vals+1;
        if($size>=2){
            $fiap_pt_path = $vals[0];
            $hardlink_path = $vals[1];
            $type = $vals[3];
            print "\t\"".$fiap_pt_path."\":\n\t{\n\t\t\"hardlink\":\"".$hardlink_path."\",";
            print "\n\t\t\"type\":\"".$type."\"";
            if($size==4 && $vals[2] ne "" ){
                print ",\n\t\t\"symlinks\":[";
                @symlink_paths = split(";", $vals[2]);
                if($#symlink_paths+1>1){
                    for($i=0; $i<=$#symlink_paths; $i++){
                        print "\"".$symlink_paths[$i]."\"";
                        if($i != $#symlink_paths){
                            print ",";
                        }
                    }
                } else {
                    print "\"".$symlink_paths[0]."\"";
                }
                print "]\n\t},\n";
            } else {
                print "\n\t},\n";
            }
        }
    }
}
close FILE;
print "\n}";
