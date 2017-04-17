//------------------------------------------------------------------------------
// This is a command-line app to convert a csv file to a json file.
// The json file is suitable for solr import.
//
// Sample csv file:
//    head0, head1, head2, ...
//    val00, val01, val02, ...
//    val10, val11, val12, ...
//    ...
//
// Sample json output file:
//    [
//      {"head0": "val00", "head1": "val01", "head2": "val02"},
//      {"head0": "val10", "head1": "val11", "head2": "val12"},
//    ]
//
// Correct command-line useage:
//    csv2json.js input_file output_file [sendMsgFunction]
//
// For csv -> json conversion with files, you can use:
//    CSV_2_JSON_Files(inFile, outFile, msgFunction);

// For csv -> json conversion with streams, you can use:
//    CSV_2_JSON_Streams(inStream, outStream, msgFunction);
//------------------------------------------------------------------------------

// handle "csv2json infile outfile" or "csv2json infile outfile [sendMsgFunction]"
if ((process.argv.length != 3) && (process.argv.length != 4)) {
    WriteCorrectUseage();
    return;
}

// make this accessible to other modules
module.exports = CSV_2_JSON_Files;

// log the names of the input and output files, and the rabbitMQ message function
var inFile = process.argv[2];
var outFile = process.argv[3];
var msgFunction = null;
if (process.argv.length === 5) {
    msgFunction = process.argv[4];
}
console.log("\ninFile = " + inFile);
console.log("outFile = " + outFile);
console.log("msgFunction = " + msgFunction + '\n');

// Do the csv -> json conversion on files.
CSV_2_JSON_Files(inFile, outFile, msgFunction);

//------------------------------------------------------------------------------
// This for converting a csv inFile to a json outFile.
//------------------------------------------------------------------------------
function CSV_2_JSON_Files(inFile, outFile, msgFunction) {
    // Convert the input file to an input stream.
    var fs = require('fs');
    var inStream = fs.createReadStream(inFile, 'utf8');
    inStream.on('error', function (err) {
        console.log("Unable to read input file:\n  " + inFile);
        WriteCorrectUseage();
        return;
    });
    
    // Convert the output file to an output stream.
    var outStream = fs.createWriteStream(outFile, 'utf8');
    outStream.on('error', function (err) {
        console.log("Unable to write to output file:\n  " + outFile);
        WriteCorrectUseage();
        return;
    });
    
    // Do the csv -> json conversion on streams.
    CSV_2_JSON_Streams(inStream, outStream, msgFunction);
}

//------------------------------------------------------------------------------
// This is for converting a csv inStream to a json outStream.
//------------------------------------------------------------------------------
function CSV_2_JSON_Streams(inStream, outStream, msgFunction) {
    var inStreamInfo = { lineCount: 0, header: [] };

    // Use the "readline" module of node.js to read one line at a time from the inStream
    var lineReader = require('readline'). createInterface({ input: inStream });
    
    // handle readline's end-of-line events    
    lineReader.on('line', function (aLine) {
        ProcessOneLine(aLine, inStream, inStreamInfo, outStream);
    });
    
    // handle readline's end-of-stream events
    lineReader.on('close', function () {
        FinishProcessing(inStream, inStreamInfo, outStream, msgFunction);
    });

}

//------------------------------------------------------------------------------
// Input: "aLine" is a single line from a csv file:
//    head0, head1, head2, ...
//    val00, val01, val02, ...
//    val10, val11, val12, ...
//    ...
//
// Output: json formated for solr:
//    [
//      {"head0": "val00", "head1": "val01", "head2": "val02"},
//      {"head0": "val10", "head1": "val11", "head2": "val12"},
//    ]
//------------------------------------------------------------------------------
function ProcessOneLine(aLine, inStream, inStreamInfo, outStream) {
    // strip white-space from line
    aLine = aLine.split(' ').join('');
    
    // split the csv line into an array-of-words
    //var array = aLine.split(",");

    // use 'csv-parse' instead of splitting the line at commas
    var parse = require('csv-parse/lib/sync');
    var data = parse(aLine);
    var array = data[0];
    
    // if we're reading the first line of the file
    if (inStreamInfo.lineCount === 0) {
        // save the the header
        inStreamInfo.header = array;
        // '[' precedes the (name, value) pairs for each line
        if (array.length > 1) {
            myWrite(inStream, outStream, "[\n", false);
        }
    }
    else {
        // ',' follows the (name, value) pairs for each line
        if (inStreamInfo.lineCount > 1) {
            myWrite(inStream, outStream, ",\n", false);
        }
        // create the (name, value) pairs for each line.
        var pairs = new Object();
        for (var i = 0; i < array.length; i++) {
            var name = inStreamInfo.header[i];
            pairs[name] = array[i];
        }
        myWrite(inStream, outStream, JSON.stringify(pairs), false);
    }
    inStreamInfo.lineCount++;
}

//------------------------------------------------------------------------------
// When reading completes, finish the outstream.
//------------------------------------------------------------------------------
function FinishProcessing(inStream, inStreamInfo, outStream, msgFunction) {
    // terminate the array of (name, value) pairs on each line with a ']'.
    if (inStreamInfo.header.length > 1) {
        myWrite(inStream, outStream, "\n]\n", true);
    }
    console.log("Converted csv to json:\n" + outFile);

    // send a msg to rabbitMQ
    if (msgFunction != null) {
        msgFunction("done");
    }
}

//-----------------------------------------------------------------------------------
// write this if command line parameters are not properly formatted
//-----------------------------------------------------------------------------------
function WriteCorrectUseage() {
    console.log("\n\nCorrect Useage:\n");
    console.log("  csv2json.js input_file output_file [sendMsgFunction]");
}

var write_counter = 0;
var out_string = "";
//-----------------------------------------------------------------------------------
// pause the input stream periodically so the output stream doesn't grow too large.
// this prevents huge memory use for very large files.
// buffering output lines in out_string, also helped with memory useage and speed.
//-----------------------------------------------------------------------------------
function myWrite(inStream, outStream, string, finalWrite) {
    out_string += string;
    write_counter++;
    if ((write_counter === 300) || (finalWrite)) {
        // pause the instream until the outstream clears
        inStream.pause();
        outStream.write(out_string, function () {
            inStream.resume();
        });
        write_counter = 0;
        out_string = "";
    }
}
