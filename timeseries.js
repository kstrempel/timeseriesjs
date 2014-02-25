
var _ = require('highland'),
    fs = require('fs'),
    async = require('async'),
    numeral = require('numeral'),
    util = require('util');

var start = new Date();

function toLines() {

    return function(err, x, push, next){
        if (x === _.nil) {
            push(null, _.nil);
        }
        else {
            _(x.split("\n")).each(function(line){
                var parts = line.split("\t");
                if(parts.length==2)
                    push(null, [parts[0], parseFloat(parts[1])]);
            });

            next();
        }
    }
}


function toGroups(){

    var grouping = { time : 0, values : []};

    return function (err, x, push, next) {
        if (x === _.nil) {
            push(null,grouping);
            push(null, _.nil);
            console.log( (new Date()).getTime() - start.getTime() );
        }
        else {
            var time = x[0];
            var value = x[1];

            if(grouping.time == time){
                if(value < grouping.min)
                  grouping.min = value
                else
                  if(value > grouping.max)
                    grouping.max = value;

                grouping.count += 1;
                grouping.sum += value;
            }else{
                push(null,grouping);
              grouping = { time : time, value : value, min : value, max : value, count : 1, sum : value };
            }

            next();
        }
    }
}

function getStream(fileToLoad){
    var stream = fs.createReadStream(fileToLoad, {
            flags : 'r',
            encoding : 'utf-8',
            fd : null
        });

    return _(stream).consume(toLines()).consume(toGroups());
}

function main(){

    if(process.argv.length != 3){
        console.log("usage <filename to import>")
        process.exit(0);
    }
    var fileToLoad = process.argv[2];

    var iter = getStream(fileToLoad);
    console.log("Time       Value  N_O Roll_Sum Min_Value Max_Value");
    console.log("---------------------------------------------------");

    iter.each(function(data){
      console.log(util.format("%s %s %s  %s  %s   %s",data.time,
                              data.value,
                              data.count,
                              data.sum,
                              data.min,
                              data.max));
    });
}

main();
