var assert = require('assert');
var restify = require('restify');
var request = require('request');
var util = require('util');
var config = require('config');

var client = function (url) {
    return restify.createJsonClient({
        url: url,
        version: '~1.0'
    });
};

var DoGet = function (url, params, internalAccessToken, callback) {
    var httpUrl = util.format('%s%s', url, params);
    var accessToken = util.format('Bearer %s',config.Services.accessToken);
    console.log('DoGet:: %s', httpUrl);
    var options = {
        url: httpUrl,
        method: 'GET',
        headers: {
            'content-type': 'application/json',
            'authorization': accessToken,
            'companyinfo': internalAccessToken
        }
    };
    try {
        request(options, function optionalCallback(err, httpResponse, body) {
            if (err) {
                console.log('upload failed:', err);
            }
            var jBody = JSON.parse(body);
            console.log('Server returned: %j', jBody);
            callback(err, httpResponse, jBody);
        });
    }catch(ex){
        callback(ex, undefined, undefined);
    }
};

var DoPost = function (serviceurl, postData, internalAccessToken, callback) {
    var jsonStr = JSON.stringify(postData);
    var accessToken = util.format('Bearer %s',config.Services.accessToken);
    var options = {
        url: serviceurl,
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            'authorization': accessToken,
            'companyinfo': internalAccessToken
        },
        body: jsonStr
    };
    request.post(options, function optionalCallback(err, httpResponse, body) {
        if (err) {
            console.log('upload failed:', err);
        }
        console.log('Server returned: %j', body);
        callback(err, httpResponse, body);
    });
};

var DoGetSync = function (url, params) {
    client(url).get(params, function (err, req, res, obj) {
        assert.ifError(err);
        console.log('Server returned: %j', obj);
        return obj;
    });
};

var DoPostSync = function (url, postData) {
    client(url).post(postData, function (err, req, res, obj) {
        assert.ifError(err);
        console.log('Server returned: %j', obj);
        return obj;
    });
};

var DoPostDirect = function (serviceurl, postData, callback) {
    var jsonStr = JSON.stringify(postData);
    var options = {
        url: serviceurl,
        method: 'POST',
        headers: {
            'content-type': 'application/json'
        },
        body: jsonStr
    };
    request.post(options, function optionalCallback(err, httpResponse, body) {
        if (err) {
            console.log('upload failed:', err);
        }
        console.log('Server returned: %j', body);
        callback(err, httpResponse, body);
    });
};

var DoGetDirect = function (serviceurl, postData, callback) {
    var jsonStr = JSON.stringify(postData);
    var httpUrl = util.format('%s? %s', serviceurl, jsonStr);
    console.log('RouteRequest:: %s', httpUrl);
    var options = {
        url: httpUrl,
        headers: {
            'content-type': 'text/plain'
        }
    };
    request(options, function optionalCallback(err, httpResponse, body) {
        if (err) {
            console.log('upload failed:', err);
        }
        console.log('Server returned: %j', body);
        callback(err, httpResponse, body);
    });
};

var PickResource = function (url, params, callback) {
    var httpUrl = util.format('%s%s', url, params);
    var accessToken = util.format('Bearer %s',config.Services.accessToken);
    console.log('DoGet:: %s', httpUrl);
    var options = {
        url: httpUrl,
        method: 'GET'
    };
    try {
        request(options, function optionalCallback(err, httpResponse, body) {
            if (err) {
                console.log('upload failed:', err);
            }
            console.log('Server returned: %s', body);
            callback(err, httpResponse, body);
        });
    }catch(ex){
        callback(ex, undefined, undefined);
    }
};


module.exports.DoGet = DoGet;
module.exports.DoPost = DoPost;
module.exports.DoGetSync = DoGetSync;
module.exports.DoPostSync = DoPostSync;
module.exports.DoPostDirect = DoPostDirect;
module.exports.DoGetDirect = DoGetDirect;
module.exports.PickResource = PickResource;