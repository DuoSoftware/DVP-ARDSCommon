/**
 * Created by Heshan.i on 3/17/2016.
 */

//Array.prototype.inArray = function(comparer) {
//    for(var i=0; i < this.length; i++) {
//        if(comparer(this[i])) return true;
//    }
//    return false;
//};

// adds an element to the array if it does not already exist using a comparer
// function
//Array.prototype.pushIfNotExist = function(element, comparer) {
//    if (!this.inArray(comparer)) {
//        this.push(element);
//    }
//};


var util = require('util');

var sortstring = function (a, b) {
    a = a.toLowerCase();
    b = b.toLowerCase();
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
};

var sortInt = function (a, b) {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
};

var sortData = function (array) {
    return array.sort(sortstring);
};

var sortIntArray = function(array){
    return array.sort(sortInt);
};

var RemoveTagFromTagStr = function(tagStr, tag, callback){
    var splitVal = tagStr.split(":");

    for(var i=0; i<splitVal.length; i++){
        if(splitVal[i].search(/^(tag)[^\s]*/) != -1){
            splitVal.splice(i,1);
        }
        if(splitVal[i].search(tag) != -1){
            splitVal.splice(i,1);
        }
    }

    var newTags = util.format('tag:%s', splitVal.join(":"));
    console.log(newTags);
    callback(newTags);
};

var RemoveTagsFromTagArray = function(tagStr, tags, callback){
    var splitVal = tagStr.split(":");
    if(Array.isArray(tags)) {
        for (var j = 0; j < tags.length; j++) {
            for (var i = 0; i < splitVal.length; i++) {
                if (splitVal[i].search(/^(tag)[^\s]*/) != -1) {
                    splitVal.splice(i, 1);
                }
                if (splitVal[i].search(tags[j]) != -1) {
                    splitVal.splice(i, 1);
                }
            }
        }
    }

    callback(splitVal);
};

var AppendNewCompanyTagStr = function(tagStr, company, callback){
    var splitVal = tagStr.split(":");
    var companyIds = [];

    for(var i=0; i<splitVal.length; i++){
        if(splitVal[i].search(/^(tag)[^\s]*/) != -1){
            splitVal.splice(i,1);
        }
        if(splitVal[i].search(/^(company_[0-9]*)[^\s]*/) != -1){
            companyIds.push(splitVal[i]);
            splitVal.splice(i,1);
        }
        if(splitVal[i].search(/^(attribute_)[^\s]*/) != -1){
            splitVal.splice(i,1);
        }
    }
    companyIds.push(company);
    companyIds = UniqueArray(companyIds);
    var sortedCompanyIds = sortIntArray(companyIds);

    var newTags = util.format('tag:%s:%s', sortedCompanyIds.join(":"), splitVal.join(":"));
    console.log(newTags);
    callback(newTags);
};

var AppendNewCompanyTagArray = function(tagStr, company, callback){
    var splitVal = tagStr.split(":");
    var companyIds = [];

    for(var i=0; i<splitVal.length; i++){
        if(splitVal[i].search(/^(tag)[^\s]*/) != -1){
            splitVal.splice(i,1);
        }
        if(splitVal[i].search(/^(company_[0-9]*)[^\s]*/) != -1){
            companyIds.push(splitVal[i]);
            splitVal.splice(i,1);
        }
        if(splitVal[i].search(/^(attribute_)[^\s]*/) != -1){
            splitVal.splice(i,1);
        }
    }
    companyIds.push(company);
    companyIds = UniqueArray(companyIds);
    var sortedCompanyIds = sortIntArray(companyIds);
    var newTagArray = sortedCompanyIds.concat(splitVal);
    callback(newTagArray);
};


var GetSortedCompanyTagArray = function(tagStr, callback){
    var splitVal = tagStr.split(":");
    var companyIds = [];

    for(var i=0; i<splitVal.length; i++){
        if(splitVal[i].search(/^(company_[0-9]*)[^\s]*/) != -1){
            companyIds.push(splitVal[i]);
        }
    }
    companyIds = UniqueArray(companyIds);
    var sortedCompanyIds = sortIntArray(companyIds);

    callback(sortedCompanyIds);
};

var ConvertTagStrToArray = function(tagStr, callback){
    var splitVal = tagStr.split(":");

    for(var i=0; i<splitVal.length; i++){
        if(splitVal[i].search(/^(tag)[^\s]*/) != -1){
            splitVal.splice(i,1);
        }
    }
    callback(splitVal);
};

var UniqueArray = function(array) {
    var processed = [];
    for (var i=array.length-1; i>=0; i--) {
        if (array[i]!= null) {
            if (processed.indexOf(array[i])<0) {
                processed.push(array[i]);
            } else {
                array.splice(i, 1);
            }
        }
    }
    return array;
};

var UniqueObjectArray = function(array, field) {
    var processed = [];
    for (var i=array.length-1; i>=0; i--) {
        if (array[i].hasOwnProperty(field)) {
            if (processed.indexOf(array[i][field])<0) {
                processed.push(array[i][field]);
            } else {
                array.splice(i, 1);
            }
        }
    }
    return array;
};

var RemoveItemFromObjectArray = function(array, field, value) {
    for (var i=array.length-1; i>=0; i--) {
        if (array[i].hasOwnProperty(field)) {
            if (array[i][field] == value) {
                array.splice(i, 1);
                break;
            }
        }
    }
    return array;
};


module.exports.sortData = sortData;
module.exports.sortIntArray = sortIntArray;
module.exports.AppendNewCompanyTagStr = AppendNewCompanyTagStr;
module.exports.AppendNewCompanyTagArray = AppendNewCompanyTagArray;
module.exports.GetSortedCompanyTagArray = GetSortedCompanyTagArray;
module.exports.ConvertTagStrToArray = ConvertTagStrToArray;
module.exports.RemoveTagFromTagStr = RemoveTagFromTagStr;
module.exports.RemoveTagsFromTagArray = RemoveTagsFromTagArray;
module.exports.RemoveItemFromObjectArray = RemoveItemFromObjectArray;
module.exports.UniqueObjectArray = UniqueObjectArray;
module.exports.UniqueArray = UniqueArray;