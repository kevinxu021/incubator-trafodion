my @version = split(/\./, $ARGV[0]);

my $major = @version[0];
my $minor = @version[1];
my $sp = @version[2];

my $revision = 0;

unless ( $major =~ /^\d+$/ && $minor =~ /^\d+$/ && $sp =~ /^\d+$/ && $revision =~ /^\d+$/ ) {
    print "Error: Invalid version on input\n";
    exit 1;

}


my @resource_files = ("TranslationDll/TranslationDll.rc",
    "drvr35adm/drvr35adm.rc",
    "drvr35/TCPIPV4/TCPIPV4.RC",
    "drvr35/TCPIPV6/TCPIPV6.RC",
    "drvr35/drvr35.rc",
    "Drvr35Res/Drvr35Res.rc",
    "../Install/SetCertificateDirReg/SetCertificateDirReg/SetCertificateDirReg.rc",
    "drvr35/cdatasource.cpp",
    "drvr35adm/drvr35adm.h"
    );

$outfile=$infile . "\.update_version_temp";

print "Updating Version to $major.$minor.$sp\n";

sub update_file {
    my $infile = $_[0];
    print  "Update " , $infile, "\n";
    my $outfile = $infile + '.tmp';
    open( INFILE, $infile ) or die "Error: Can't open $infile - $!";
    open( OUTFILE, ">$outfile" ) or die "Error: Can't open $outfile - $!";

    while ( <INFILE> ) {
        if ( /FILEVERSION|PRODUCTVERSION/ ) {
            s/(\d+),(\d+),(\d+),(\d+)/$major,$minor,$sp,$revision/;
            print OUTFILE;
        }
        elsif( /"ProductVersion|FileVersion"/ ) {
            s/, "(\d+), (\d+), (\d+), (\d+)"/, "$major,$minor,$sp,$revision"/;
            s/, "(\d+)\.(\d+)\.(\d+)\.(\d+)"/, "$major.$minor.$sp.$revision"/;
            print OUTFILE;
        }
        elsif( /SOFTWARE\\\\ODBC\\\\ODBCINST.INI\\\\TRAF ODBC /) {
        	s/(\d+)\.(\d+)/$major.$minor/;
        	print OUTFILE;
        }
        elsif( /DRIVER_NAME\[\] = "TRAF ODBC / ) {
        	s/(\d+)\.(\d+)/$major.$minor/;
        	print OUTFILE;
        }
        else {
            print OUTFILE;
        }
    }

    close( INFILE ) or warn "Warning: Can't close $infile - $!";
    close( OUTFILE ) or warn "Warning: Can't close $outfile - $!";

    unless ( rename $outfile, $infile ) {

        print "Error: Updating Version for $infile failed.\n";
        exit 1;
    }
}

foreach $file (@resource_files) {
    update_file $file
}

exit 0;