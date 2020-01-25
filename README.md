# open62541_sqlite
This is a demo for using open62541 to access historical data from an sqlite database.

The code written and build with Visual Studio 16 on Windows 10 but it should easily be ported to Linux. There are a few date time conversion routines that use MS specific runtime APIs but with a precompiler conditional it could be made cross-platform, at least for Linux and MS.

The project includes an sqlite database that simulates a measured values database. It contains values for only a single measuring point and the ID of that measuring point is hardcoded for this demo!

This project uses a bundled source version of open62541 which is not up-to-date! Unfortunately I couldn't find a newer bundled version. You can alternatively use it with a binary release of open62541 but that would make it platform-dependent. You can of course check out the entire source of open62541 but I preferred to have only two files to include which would compile under Windows and Linux.

The original discussion acompanying the development of this demo can be found here:
https://github.com/open62541/open62541/issues/2396

As a client for testing the server I uses OPC UA Viewer from here: http://www.commsvr.com/
This client has a function for reading historical data. Note you have to uncheck the option
to return "bounds"! This demo server does not support returning bounds!

This is just a minimal demo. It returns the data for this specific setup but there is certainly a lot more work neccessary to make it flexible and usable in other scenarios.

Feel free to submit push request to improve the code!

