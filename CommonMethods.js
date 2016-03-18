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

var sortstring = function (a, b) {
    a = a.toLowerCase();
    b = b.toLowerCase();
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
};

var sortData = function (array) {
    return array.sort(sortstring);
};

module.exports.sortData = sortData;