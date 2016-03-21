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

var AppendCompanyTag = function(tagStr, company, callback){
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
    }
    companyIds.push(company);
    var sortedCompanyIds = sortIntArray(companyIds);

    var newTags = util.format('tag:%s:%s', sortedCompanyIds.join(":"), splitVal.join(":"));
    console.log(newTags);
    callback(newTags);
};


module.exports.sortData = sortData;
module.exports.sortIntArray = sortIntArray;
module.exports.AppendCompanyTag = AppendCompanyTag;