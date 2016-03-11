var infoLogger = require('./InformationLogger.js');

var ValidateAuthToken = function (req, callback) {
    var tenantId = 1;
    var companyId = 1;
    try {
        if(req.header('companyinfo')){
            var internalAuthHeader = req.header('companyinfo');
            var authInfo = internalAuthHeader.split(":");

            if (authInfo.length >= 2) {
                tenantId = authInfo[0];
                companyId = authInfo[1];
            }
            callback(null, companyId, tenantId);
        }else if(req.user.company && req.user.tenant){
            tenantId = req.user.tenant;
            companyId = req.user.company;
            callback(null, companyId, tenantId);
        }else {
            callback(new Error("Invalid company or tenant"), companyId, tenantId);
        }
    }
    catch (ex) {
        infoLogger.ReqResLogger.log('error', 'Exception occurred -  Data - %s ', "authorization", ex);
        callback(ex, companyId, tenantId);
    }
};


module.exports.ValidateAuthToken = ValidateAuthToken;