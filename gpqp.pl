#!/usr/bin/perl -w
#=================================================================================================
#Script Name : gpqp.pl
#Purpose : To check pg_log for select statements cost
#Usage : gpqp.pl -I <log file name> -O <output file>  
#Dependency : gpqpreport_cat.conf, directory -> $PLANLOC 
#Author : Pavan Kumar
#Date Creation : 29th April, 2009
#Last Modified : 6th Aug, 2009 
#Change History : 11-05-09 - Added resource queue limit flag for query rejected due to cost limir on resource queue
#		  13-05-09 - Added report category
#		  16-05-09 - Report Category addition done 
#		  18-05-09 - replaced split with substr for select line : to rectify syntax error due to missing select
#		  06-08-09 - added group by and where values
#=================================================================================================

use Getopt::Std;
getopt('IOH');           # I,O,H are valid switches

my $CONFLOC = '/app/gpadmin/pk/script';
my $PLANLOC = '/app/gpadmin/pk/cost'; 

open(SEQ, "$opt_I") or die "Can't open input file log : $!\n";  ##open for read
open(RC, "/app/gpadmin/pk/script/gpqpreport_cat.conf") or die "Can't open input file RC : $!\n";  ##open for read
open(SEQ1,">$opt_O") or die "Can't open output file in gp_data : $!\n"; ##open for write
##open(SEQ2,">$opt_H") or die "Can't open output file : $!\n"; ##open for write
##open(SEQ3,"/tmp/gpolap.21447.plan") or die "Can't open input file plan : $!\n";

my @ta;
my @tb;
my @tc;
my @td;
my @te;
my @tf;
my @tg;
my @badprocarray;
my @resq;
my @tmp;
my @tmp1;
my $who = "";
my $rest = "";
my @fields;
my %RepCat;
my $facttable = 0;
my @tables;
my $line = "";
my $line2 = "";
my $linefound = 0;
my $linenum = 0;
my $linenumchange = 0;
my $selectfound = 0;
my $linefoundb = 0;
my $message1 = "";
my $username = "";
my $linep = "";
my $planline = "";
my $querycost = 0;
my $procid = 0;
my $exectime = "";
my $prevline = "";
my $mstruser = "";
my $procline = "";
my $badproc = 0;
my $badproctmp = 0;
my $resource = "";
my $badcount = 0;
my $resqcount = 0;
my $baddesc = "";
my $arraystring = "";
my $fact = "";
my $reptype = "";
my $reptablestring = "";
my $tablefound = 0;
my $reportname = "";
my $fact_t = "";
my $fact_t_prev = "";
my $repcount = 0;
my @match;
my $matchcount = 0;
my $t_count = 0;
my $t_count_priv = 0;
my $r_name = "";
my $r_name_priv = "";
my $repcount_priv = 0;
my $selline = "";
my @duplicate;
my $dup = 0;
my $procuser = "";
my %RepTime;
my $durationline = "";
my $time_taken = 0.0;
my @time_ms;
my $exec_duration = 0;
my $groupvalue = "";
my $groupfound = 0;
my $groupline = "";
my $wherevalue = "";
my $wherefound = 0;
my $whereline = "";
my @group ;
my @group1;

##my $mailid = "pavan.kumar@pantaloon.com";

open(REM, "| /usr/bin/rm $PLANLOC/*.*") || die "REM failed $!\n";
close(REM);

while ($line = <SEQ> ) {
	if ( $line =~m/(server)\s+(process)\s+(\()(PID)\s+(\d+)(\))\s+(was)\s+(terminated)\s+(by)\s+(signal)\s+(11)/m) { push(@badprocarray, $5); $badcount++;}
	if ( $line =~m/(fgdwprd)(\|)(\d+)(\|)(\d+)(:)(-)(ERROR)(:)\s+(statement)\s+(requires)\s+(more)\s+(resources)\s+(than)/m ) { push(@resq,$3); $resqcount++;}
	if ( $line =~m/(duration)/m ) {
	@duration = split /\|/,$line;  ## get the user and processid
	$procid = $duration[3];

	$durationline = substr($line,index($line,"duration"));
	print "$durationline\n";
	@time_ms = split ' ', $durationline;
	$time_taken = $time_ms[1];
	print "Procid Time Taken --$procid --  $time_taken\n";

	$RepTime{$procid} = $time_taken;
	}
}
$procid="";
for my $reptime1 ( keys %RepTime ) {
                                        print "Report Time Procid $reptime1 : ";
                                       my  $reptime_duration = $RepTime{$reptime1};
                                        print "Report Time: $reptime_duration\n";
}


close(SEQ);
$line = "";
##Populate the report category data structure
while ($line  = <RC> ) {
	($who, $rest) = split /:\s*/, $line , 2;
	@fields = split ' ', $rest;
	$RepCat{$who} = [ @fields ];
}
close(RC);
for $reptype ( keys %RepCat ) {
                                        print "Report TYPE $reptype: ";
                                        $reptablestring = join(' ',@{$RepCat{$reptype}});
                                        print "Report Type Table Members: $reptablestring\n";
}
$line = "";

open(SEQ, "$opt_I") or die "Can't open input file log : $!\n";  ##open for read

LINE: while ($line = <SEQ> ) {
	if ( $line =~m/(MSTR)\s+(USER)\s+(NAME)\s+(-)\s+(\w+)/m ) { $mstruser = $5; next LINE; } ##capture the mstr username which is before select stmt begins
	if ( $linefound == 0 ) {
        	if ($line =~m/\s+(IST\|)/m) {
			if ( $line =~m/(select)\s+/m ) {
				if (( $line =~m/(select)\s+(version)/m ) || ( $line =~m/(\;)/m)) { $linefound = 0; }	
				elsif (( $line =~m/(select)\s+(oid)/m ) || ( $line =~m/(select)\s+(count)/m ) || ( $line =~m/(select)\s+(\;)/m)) { $linefound = 0; }
				elsif (( $line =~m/(insert)\s+/i ) || ( $line =~m/(delete)\s+/i ) || ( $line =~m/(explain)\s+/i )) { $linefound = 0; }
				else  {
					@ta = split /\|/,$line;  ## get the user and processid
					##@tb = split /:/,$ta[4]; ## get the line num and select part,
			   		$username = $ta[1];
					$procid = $ta[3];
				###	$procuser = $username.$procid;
				###	foreach my $val (@Duplicate) {
			         ###              print  "$val\n";
				###		if ( $procuser =~m/($val)/m ) { $dup = 1;};
               			###	}
				###	if ( $dup == 0 ) { push(@duplicate,$procuser); }
				##	$procuser = "";
					##$exectime = $ta[0];	
					$exectime = substr($ta[0],0,16);
					$exec_duration = $RepTime{$procid};
					$selline = substr($line,index($line,"select"));
					print "SUBSTR SEL line: $selline\n";
					if ( $badcount > 0 ) {	
						if ($badprocarray[0] == $procid ) { $badproc = 1; shift(@badprocarray); $badcount--; } ##check if query crashed the db
					}	

					if ( $badproc == 1 ) { open(SEQ2,">$PLANLOC/$ta[1].$ta[3].bad") or die "Can't open cost output file : $!\n";} ##open for write
			 		else {open(SEQ2,">$PLANLOC/$ta[1].$ta[3]") or die "Can't open cost output file : $!\n";} ##open for write
					print SEQ2 "explain\n";  ##select part of first line 
					##print SEQ2 "$tb[3]\n";  ##select part of first line
					print SEQ2 "$selline";  ##select part of first line
					$linefound = 1;
				}
			}	
		}
	}
	else {
		if ( $line !~m/(IST\|)/m ) {
			print SEQ2 "$line";
### Find the Fact table--------------------------------
			if ( $line =~m/(from)\s+(rds\.)(\w+)/m ) {
			##	print "$line\n";
				$fact = $3;
				##print "Table ... $fact\n\n";
				if ( $fact =~/.*?ft_/) { 
					print "Ft_Table ... $fact\n";
					##foreach my $val (@tmp) 	print "$val\n"; 	}
					if ( $facttable == 0 ) { push(@tables,$fact); $facttable++; }
					else {
						$arraystring = join(' ',@tables);
						print "Array Output $arraystring $facttable\n";
						if ($arraystring !~m/(\b$fact\b)m/) { 
							push(@tables,$fact); 
							$facttable++;
							print "\nTABLE $fact not FOUND in array $facttable\n"; 
						}
					}			
				}
			}
####End of Fact Table find -----------------------------------------
##050809  Below block finds the group by values-------------------------
			if ( $groupfound == 0 ) {
				if ( $line =~m/(group)\s+(by)/m ) {
					$groupline = substr($line,(index($line,"by")+2),length($line));
					chomp ($groupline);
					$groupline =~ s/^\s+//g;
					$groupline =~ s/\s+$//g;
					##@group = split /\./,$line;  
					##@group1 = split ' ',$group[1];
					##$groupvalue = $group1[0];
					$groupvalue = $groupline;
					$groupfound = 1;
					print "Group By $line\n";	
					print "$groupvalue\n";
				}
			}
			else {
				if (( $line =~m/(order)/m ) || ( $line =~m/(\()/m ) || ( $line =~m/(\))/m ) || ( $line =~m/(having)/m )) {
					$groupfound = 0;
				}
				else {
					chomp ($line);
					$line =~ s/^\s+//g; ## Removes blank spaces from begining
                                        $line =~ s/\s+$//g;  ## Removes blank spaces from end
					##@group = split /\./,$line;  ## get the user and processid                                                              
					##@group1 = split ' ',$group[1];
                                        $groupvalue = "$groupvalue ".$line;	
                                        print "Group By Next $line\n";
					print "$groupvalue\n";
				}	

			}
###End of Group by block
### Start of where block			###################################################
			if ( $wherefound == 0 ) {
                                if ( $line =~m/(where)/m ) {
					$whereline = substr($line,(index($line,"where")+5),length($line));
					chomp ($whereline);
					$whereline =~ s/^\s+//g;
					$whereline =~ s/\s+$//g;
                                        ##$wherevalue = $line;
                                        $wherevalue = $whereline;
                                        $wherefound = 1;
                                        print "Where First Line $line\n";
                                        print "$wherevalue\n";
                                }
                        }
                        else {
                                if (( $line =~m/(order)/m ) || ( $line =~m/(group)/m ) || ( $line =~m/(select)/m )) {
                                        $wherefound = 0;
                                }
                                else {
					chomp ( $line);
					$line =~ s/^\s+//g; ## Removes blank spaces from begining 
                                        $line =~ s/\s+$//g;  ## Removes blank spaces from end
                                        $wherevalue = "$wherevalue".$line;
				##	$wherevalue = "$wherevalue ".substr($line,0,length($line));
                                        print "Where Next $line\n";
                                        print "$wherevalue\n";
                                }

                        }
###End of Where Block##########################################
			if ( $line =~m/(\;)/m ) { $linefound = 0; $groupfound = 0; $wherefound = 0; close(SEQ2); }
		}
		elsif ( $line =~m/(IST\|)/m ) {
			if ($resqcount > 0 ) {
				if ( $resq[0] == $procid ) { 
					$resource = "rlimit";
					shift(@resq);
					$resqcount--;
					next LINE;
				} ## check for the resourceq limit breach
			 		##next LINE is to remove the duplicate entry in output file for procid rejected due to resource limit.
			}
			$linefound = 0;
			$groupfound = 0;
			$wherefound = 0;
			close(SEQ2);
#### Below block gets the Report Category
			if ($facttable > 0 ) {
				for $reptype ( keys %RepCat ) {
					$reptablestring = join(' ',@tables);
					print "Fact Table Search String:  $reptablestring\n";
					if ($tablefound > 0 ) { 
						push(@match,$reportname);
						$matchcount++;
						push(@match, $tablefound);
						$matchcount++;
						$repcount++;
						push(@match,$repcount);
						$matchcount++;
						$tablefound = 0;
						print "REPCOUNT: $repcount\n";
						print "Report TYPE $reptype: ";
					}
						for my $i (0..$#{ $RepCat{$reptype} } ) {
							$repcount = $i;
							$fact_t = $RepCat{$reptype}[$i];
							print "Fact_T $fact_t\n";
								if ( $reptablestring =~/\b$fact_t\b/ ) { 
									$tablefound++;
									$reportname = $reptype ; 
									$fact_t_prev = $fact_t;
									print "Final Report Type: $reportname\n"; }
						}
				}
				$repcount = 0;
				my $t_count_max = 0;
				while ( $matchcount > 0 ) {
					##$t_count_priv = $t_count;
					##$repcount_priv = $repcount;
					$repcount = pop(@match);
					$matchcount--;
					$t_count = pop(@match);
					$matchcount--;
					$r_name = pop(@match);
					$matchcount--;
					if (( $t_count >= $t_count_max ) && ( $t_count == $repcount )) {
						$t_count_max = $t_count; 
						$reportname = $r_name;
						print "TRcount: $t_count\n";
                                                print "ReRpCount: $repcount\n";
                                                print "ProceID: $procid\n";
                                                print "ReportName: $r_name\n";
                                                print "MaxCount: $t_count_max\n";

					}
				}
				$matchcount = 0;
				$t_count = 0;
				$t_count_priv = 0;
				$r_name = "";
				$r_name_priv = "";
				$repcount = 0;
		
			} 	
### End of Report Category Block
			if ($badproc != 1 ) {
				if ( -e "$PLANLOC/$ta[1].$ta[3].plan" ) { $dup = 1; }
				else {
					open(PLAN, "| /app/greenplum-db/bin/psql -a -f $PLANLOC/$ta[1].$ta[3] -o $PLANLOC/$ta[1].$ta[3].plan") || die "plan failed $!\n";
					close(PLAN);
				} 
				open(SEQ3,"$PLANLOC/$ta[1].$ta[3].plan") or die "Can't open input file for plan: $!\n";
				while ($planline = <SEQ3> ) {
			      		if ($planline =~m/(Gather)\s+(Motion)/m) {
                				@tc = split /\=/,$planline;
	             				@td = split /\./,$tc[1];
						$querycost = $td[0]; ## cost of query
					}
				}
				close(PLAN);
				close(SEQ3);

			}
			if ($badproc == 1 ) { $baddesc = "crash" ; } 
			print "Writinting to File\n";
			if ( $querycost > 0 ) {
	print SEQ1 "$exectime\|"."$username\|"."$mstruser\|"."$procid\|"."$querycost\|"."$resource\|"."$baddesc\|"."$reportname\|"."$dup\|"."$exec_duration\|"."$groupvalue\|"."$wherevalue\n";
			}
			$mstruser = "";
			$resource = "";
			$badproc = 0;
			$baddesc = "";
			@tables = "";
			$facttable = 0;
			$fact = "";
			$fact_t = "";
			$reportname = "";
			$tablefound = 0;
			$matchcount = 0;
			$exec_duration = 0;
			$dup = 0;
			$groupvalue = "";
			$wherevalue = "";
		}
	}
$badcount = 0;
}
close(SEQ1);
close(SEQ);
##open(REM, "| /usr/bin/rm $PLANLOC/*.*") || die "REM failed $!\n";
##close(REM);
