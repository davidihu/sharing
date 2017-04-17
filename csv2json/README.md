# Overview

This command-line app converts a csv file to a json file.
The json file is suitable for solr import.

# To run from the command line

    node csv2json.js input_file output_file [sendMsgFunction]
    
    For csv -> json conversion with files, you can use:
      CSV_2_JSON_Files(inFile, outFile, msgFunction);

    For csv -> json conversion with streams, you can use:
      CSV_2_JSON_Streams(inStream, outStream, msgFunction);

# Sample input

```
    head0, head1, head2, ...
    val00, val01, val02, ...
    val10, val11, val12, ...
    ...
```
    
# Sample output

```
    [
      {"head0": "val00", "head1": "val01", "head2": "val02"}, ...
      {"head0": "val10", "head1": "val11", "head2": "val12"}, ...
    ]
```

