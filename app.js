/*jshint node:true */
'use strict';

var config = require('./config');

var mysql = require('mysql');
var Q = require('q');
var _ = require('underscore');
var request = require('request');
var path = require('path');
var fs = require('fs');

// mysql connection setup
var mysqlConnection = mysql.createConnection({
    user : config.mysql.user,
    password : config.mysql.password,
    host : config.mysql.host,
    //port : config.mysql.port,
    database : config.mysql.database
});

/**
 * Connects to the database
 * @return {Q.Promise} a promise that will be resolved once the connection
 * has been established
 */
function mysqlConnect() {
    var deferred = Q.defer();

    mysqlConnection.connect(function (error) {
        if (error) {
            deferred.reject(error);
        } else {
            console.log('MYSQL: connection established');
            deferred.resolve();
        }
    });

    return deferred.promise;
}

// some DB queries
var allPosts = 'SELECT `ID`, `post_content` FROM `wp_posts` LIMIT 1';
var rResource = /(http:\/\/(www\.)?macstories.net)?\/(stuff|wp-content\/uploads)\/.+?\.(jpe?g|gif|png|zip|rar|gz)/g;

// let's start
mysqlConnection().then(getAllPosts).then(processPosts).then(closeConnection, handleError);

/**
 * Queries the db, retrieving all the posts
 * @return {Q.Promise} a promise that will be resolved once the driver returns
 * the posts
 */
function getAllPosts() {
    var deferred = Q.defer();

    // query the db and pass over the result
    mysqlConnection.query(allPosts, function (error, rows) {
        if (error) {
            deferred.reject(error);
        } else {
            deferred.resolve(rows);
        }
    });

    return deferred.promise;
}

/**
 * Processes all the posts, breaking down the job into multiple processes
 * @param {Array[Post]} posts the array of post to process
 * @return {Q.Promise} a promise that will be resolved once all the posts have
 * been migrated
 */
function processPosts(posts) {
    // keep some stats about all the posts
    var stats = {
        resourcesFound : 0,
        resourcesProcessed : 0,
        resourcesFailed : 0
    };

    // this deferred will be resolved once the whole process completes
    // (all posts' resources have been migrated to Rackspace)
    var deferred = Q.defer();

    // each post has a promise that will be resolved once the process has been
    // completed (which means that all its resources have been downloaded,
    // uploaded to Rackspace, and the post itself has been updated in the db)
    var allPostsProcessed = posts.map(managePost);

    // update the stats every time a post has been migrated
    allPostsProcessed.forEach(function (promise, index, array) {
        promise.then(function (result) {
            stats.resourcesFound += result.found;
            stats.resourcesProcessed += result.processed;
            stats.resourcesFailed += result.failed;
        });
    });

    // once all post have been processed, 'return' to the caller with
    // the stats about the process
    Q.all(allPostsProcessed).then(function () {
        deferred.resolve(stats);
    });

    return deferred.promise;
}

/**
 * Manages a single post, finding the resources in the post and processing
 * every single resource
 * @param {Post} post the post to be processed
 * @return {Q.Promise} a promise that will be resolved once all the resources
 * have been processed
 */
function managePost(post) {
    // this is a deferred object that will take an eye on the whole process
    var deferred = Q.defer();

    // keep some stats for the current post
    var stats = {
        found : 0,
        processed : 0,
        failed : 0
    };

    // get all the (unique) resources in this post and save them in an array
    // of objects, containing both the post id and the resource URL
    var resources = findResources(post.post_content).map(function (resource) {
        return {
            postID : post.ID,
            resourceURL : resource
        };
    });

    // early exit :)
    if (resources.length === 0) {
        console.log('Post ', post.ID, ' has no resources');
        deferred.resolve(stats);
    }

    // keep a map of old resources -> new resources
    var updates = {};

    // create an array of promises, one per resource
    var allResourcesProcessed = resources.map(manageResource);

    // handle the resources result, updating the updates map if succeded
    allResourcesProcessed.forEach(function (promise, index, array) {
        promise.then(function (result) {
            stats.found++;
            if (result.success) {
                updates[result.oldURL] = result.newURL;
                stats.processed++;
            } else {
                stats.failed++;
            }
        });
    });

    // when all the post's resources have been migrated, we need to update the
    // post in the db. This deferred object will tell when it happens
    var dbUpdated = Q.defer();

    // update the db
    Q.all(allResourcesProcessed).then(function () {

        // update the post using the updates map

        dbUpdated.resolve();

    });

    // once the db has been updated, 'return' to the caller
    dbUpdated.promise.then(function () {
        deferred.resolve(stats);
    });

    return deferred.promise;
}

/**
 * Finds the unique resources in the post
 * @param {String} postContent the content of the post
 * @return {Array[String]} an array of resources
 */
function findResources(postContent) {
    return _.uniq(postContent.match(rResource), false);
}

/**
 * Manages the download and upload process of a resource
 * @param {Object} data the object representing the resource to manage
 * @param {String} data.postID the ID of the post which the resource belongs to
 * @param {String} data.resourceURL the url of the resource
 * @return {Q.Promise} a promise that will be resolved once the process completes
 * of fails (it won't get rejected)
 */
function manageResource(data) {
    // this deferred object manages the single resource process
    var deferred = Q.defer();

    downloadResource(data).then(uploadResource).then(function (newResourceURL) {
        console.log('RESOURCE:', data.resourceURL, ' -> ', newResourceURL);

        // resolve the resource's deferred object
        deferred.resolve({
            success : true,
            oldURL : data.resourceURL,
            newURL : newResourceURL
        });
    }, function (error) {
        // either the download or the upload went wrong

        // log the error
        console.error('NET: resource', data.resourceURL, ' failed due to error', error);

        deferred.resolve({
            success : false,
            oldURL : data.resourceURL,
            newURL : null
        });
    });

    return deferred.promise;
}

/**
 * Downloads a resource
 * @param {Object} data the object representing the resource to manage
 * @param {String} data.postID the ID of the post which the resource belongs to
 * @param {String} data.resourceURL the url of the resource
 * @return {Q.Promise} a promise that will be resolved with the image data or
 * reject with the error occurred
 */
function downloadResource(data) {
    console.log('NET: downloading resource', data.resourceURL);

    // this deferred object will take care of the download process
    var deferred = Q.defer();

    var remoteFilename = path.basename(data.resourceURL);
    var localFilename = '/tmp/' + data.postID + '_' + remoteFilename;

    var stream = fs.createWriteStream(localFilename);

    // download the resource
    request(data.resourceURL).pipe(stream);

    // an error occurred
    stream.on('error', function (error) {
        deferred.reject(error);
    });

    // the file has been saved
    stream.on('close', function () {
        deferred.resolve({
            URL : data.resourceURL,
            path : localFilename
        });
    });

    return deferred.promise;
}

/**
 * @param {Object} data the data of the image to upload
 * @param {String} data.URL the URL of the old resource
 * @param {Buffer} data.data the data of the resource to upload
 * @return {Q.Promise} a promise that will be resolved with new image's URL or
 * will be rejected with the error occurred
 */
function uploadResource(data) {
    console.log('NET: uploading to Rackspace', data.URL);

    var deferred = Q.defer();
    deferred.resolve('bbbb');
    return deferred.promise;
}

function updatePost() {
    var deferred = Q.defer();
    deferred.resolve();
    return deferred.promise;
}

function closeConnection(stats) {
    mysqlConnection.end(function () {
        console.log('MYSQL: connection closed');

        console.log('\n\nPROCESS COMPLETED\n', stats);
        exit(0);
    });
}

function handleError(error) {
    console.error('\n\nPROCESS ABORTED\n', error);
    exit(1);
}

function exit(code) {
    process.exit(code);
}

