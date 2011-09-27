#!/usr/bin/perl
use strict;
use warnings;
use Time::Local;
use Switch;
use DBI;
sub getDaysAgo;
sub doDatabaseInsert;

# ------
# You must change this!
# to be the path to this file (so that you can read in the properties file
chdir "/full/patht/to/this/file";

my ($uname, $pwd, $vendorID, $dateType, $reportType, $reportSubType, $databaseFilename, $daysBefore);
$daysBefore=2; #default value, can be overridden
open my $in, "report.properties" or die $!;
my %o;
while (<$in>) {
    $o{$1}=$2 while m/(\S+)=(\S+)/g;
    switch ($1) {
        case "uname" {
            $uname = $2;
        }
        case "pwd" {
            $pwd = $2;
        }
        case "vendorID" {
            $vendorID = $2;
        }
        case "dateType" {
            $dateType = $2;
        }
        case "reportType" {
            $reportType = $2;
        }
        case "reportSubType" {
            $reportSubType = $2;
        }
        case "databaseFile" {
            $databaseFilename = $2;
        }
        case "daysBefore" {
            $daysBefore = $2;
        }
    }
}

if (! defined($uname) || !defined($pwd) || !defined($vendorID) || !defined($dateType) || !defined($reportType) || !defined($reportSubType) || !defined($databaseFilename)) {
    die("a parameter was not set in report.properties");
}

my $theDate = getDaysAgo($daysBefore);
my $theCmd = "java Autoingestion $uname " . '"' . $pwd . '" ' . "$vendorID $reportType $dateType $reportSubType $theDate"; 

my $result = `$theCmd`;
if ($? != 0) {
  print "command failed: \n$!";
} else {
  my @lines = split(/\n/, $result);
  my $filename = $lines[0];
  print "\nDownloaded: " . $filename . "\n";
  my $unzipCmd = "gzip -d " . $filename;
  my $unzipRes = `$unzipCmd`;
  if ($? != 0) {
    print "failed to unzip: " . $filename . "\n";
  } else {
    #print "File: " . $newFilename . " is ready to use\n";
    my $dirName = "reports";
    if (! -d $dirName) {
      my $mkDir = `mkdir $dirName`;
    }
    $filename =~ s/...$//; #replace the last 3 chars with null
    my $newFilename = $theDate . "_" . $reportType . ".tsv";
    if (! -e $dirName . "/" . $newFilename) { #if the file already exists, don't update the db
        my $newDir = 
        my $moveCmd = "mv " . $filename . " $dirName/$newFilename"; 
        my $mvRes = `$moveCmd`;
        if ($? != 0) {
          print "failed to move " . $filename . " to $dirName/$newFilename\n";
        } else {
          print "Report is ready at: $dirName/$newFilename\n";
            if (!-e $databaseFilename) {    # if the DB doesn't exist, create it along with the db schema
                my $dbCmd = "echo '.read schema_trivial.sql' | sqlite3 $databaseFilename";
                my $execDB = `$dbCmd`;
                if ($? != 0) {
                    print 'failed to create DB with cmd: \n';
                }
            }
            # parse the results file
            my $databasResult = doDatabaseInsert($dirName, $newFilename);
        }
    } else {
        print "Report already exists, skipping DB insert\n";
    }
  }
}

sub getDaysAgo{
    my($daysAgo) = @_;
    my ($sec, $min, $hour, $mday, $mon, $year) = localtime();
    my $day_midday=timelocal(0,0,12,$mday,$mon,$year) - (24*60*60*$daysAgo);
    ($sec, $min, $hour, $mday, $mon, $year) = localtime($day_midday);
    my $returnDay= sprintf "%04d%02d%02d",$year+1900, $mon+1, $mday;
    return $returnDay;
}

sub doDatabaseInsert {
    my($dirName, $fileName) = @_;
    my $dbargs = {AutoCommit => 0,
        PrintError => 1};
    
    open (FILE, $dirName . '/' . $fileName);
    my $insertString = 'INSERT INTO sale (SKU, developer, title, units, developerProceeds, saleDate, currencyOfProceeds, customerPrice) VALUES(';
    my $dataInsert;
    my $dbh = DBI->connect("dbi:SQLite:dbname=$databaseFilename","","",$dbargs);
    while (<FILE>) {
        chomp;
        
        my ($Provider, $ProviderCountry, $SKU, $Developer, $Title, $Version, $ProductTypeIdentifier, $Units, $DeveloperProceeds, $BeginDate, $EndDate,$CustomerCurrency, $CountryCode, $CurrencyOfProceeds, $AppleIdentifier, $CustomerPrice, $PromoCode, $ParentIdentifier, $Subscription, $Period) = split("\t");
        
        next if ($SKU eq "SKU");
        
        $dataInsert = $insertString . "'$SKU', '$Developer', '$Title', '$Units', '$DeveloperProceeds', '$EndDate', '$CurrencyOfProceeds', '$CustomerPrice'); ",
        
        $dbh->do($dataInsert);
        if ($dbh->err()) { die "$DBI::errstr\n"; }        
        $dbh->commit();
    }
    
    $dbh->disconnect();
    print "Report data has been inserted into db: $databaseFilename\n";
    
}

print "\n";
exit 0;
