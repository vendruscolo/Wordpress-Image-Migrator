/*jshint node:true */
'use strict';

var config = require('./config');

var mysql = require('mysql');
var Q = require('q');
var _ = require('underscore');
var request = require('request');
var path = require('path');
var fs = require('fs');
var cloudfiles = require('cloudfiles');

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

// custom query format
mysqlConnection.config.queryFormat = function (query, values) {
  if (!values) return query;
  return query.replace(/\:(\w+)/g, function (txt, key) {
    if (values.hasOwnProperty(key)) {
      return this.escape(values[key]);
    }
    return txt;
  }.bind(this));
};

// Rackspace connection setup
var rackspaceClient = cloudfiles.createClient({
    auth : {
        username : config.rackspace.username,
        apiKey : config.rackspace.apiKey
    }
});

// some DB queries
var allPostsQuery = 'SELECT `ID`, `post_content` FROM `wp_posts`';
var updatePostContentQuery = 'UPDATE `wp_posts` SET `post_content` = :postContent WHERE ID = :postID';
var rResource = /(http:\/\/(www\.)?macstories.net)?\/(stuff|wp-content\/uploads)\/.+?\.(jpe?g|gif|png|zip|rar|gz)/g;
var rackspaceBucketName = config.rackspace.bucketName;
var rackspaceCDNURL = '';

// let's start
var processStarted = new Date();
mysqlConnect().then(rackspaceLogin).then(getCDNURL).then(getAllPosts).then(processPosts).then(closeConnection, handleError);

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

/**
 * Authenticates with Rackspace
 * @return {Q.Promise} a promise that will be resolved once the client
 * has been authenticated
 */
function rackspaceLogin() {
    var deferred = Q.defer();

    rackspaceClient.setAuth(function (error, response, auth) {
        if (error) {
            deferred.reject(error);
        } else {
            console.log('NET: Rackspace auth token received');
            deferred.resolve();
        }
    });

    return deferred.promise;
}

/**
 * Gets the CDN URL of the container
 * @return {Q.Promise} a promise that will be resolved once the CDN URL is retrieved
 */
function getCDNURL() {
    var deferred = Q.defer();

    rackspaceClient.getContainer(rackspaceBucketName, true, function (error, container) {
        if (error) {
            deferred.reject(error);
        } else {
            rackspaceCDNURL = container.cdnUri;
            deferred.resolve();
        }
    });

    return deferred.promise;
}

/**
 * Queries the db, retrieving all the posts
 * @return {Q.Promise} a promise that will be resolved once the driver returns
 * the posts
 */
function getAllPosts() {
    var deferred = Q.defer();

    // query the db and pass over the result
    mysqlConnection.query(allPostsQuery, function (error, rows) {
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
    var resources = findResources(post.post_content);

    // early exit :)
    if (resources.length === 0) {
        console.log('POST: Post', post.ID, 'has no resources');
        deferred.resolve(stats);
    }

    // keep a map of old resources -> new resources
    var updates = {};

    // create an array of promises, one per resource
    var allResourcesProcessed = resources.map(function (resource) {
        return manageResource(post.ID, resource);
    });

    // handle the resources result, updating the updates map if succeded
    allResourcesProcessed.forEach(function (promise, index, array) {
        promise.then(function (result) {
            stats.found++;
            if (result.success) {
                updates[result.oldResource] = result.newResource;
                stats.processed++;
            } else {
                stats.failed++;
            }
        });
    });

    // update the db
    Q.all(allResourcesProcessed).then(function () {

        // update the post using the updates map
        return replaceResources(post.ID, post.post_content, updates);

    }).then(function (newContent) {
        return updatePost(post.ID, newContent);
    }).then(function () {

        // ok, the post has been correctly updated in the db
        deferred.resolve(stats);

    }, function (error) {

        // whoops, an error occurred at the DB level, delete the resources from
        // Rackspace
        deleteResources();

        // but resolve anyway, so that we can continue with other posts
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
 * @param {String} postID the ID of the post which the resource belongs to
 * @param {String} resource the resource to manage
 * @return {Q.Promise} a promise that will be resolved once the process completes
 * of fails (it won't get rejected)
 */
function manageResource(postID, resource) {
    // this deferred object manages the single resource process
    var deferred = Q.defer();

    downloadResource(postID, resource).then(function (result) {
        return uploadResource(result.resource, result.path);
    }).then(function (newResource) {
        console.log('RESOURCE:', resource, ' -> ', newResource);

        // resolve the resource's deferred object
        deferred.resolve({
            success : true,
            oldResource : resource,
            newResource : newResource
        });
    }, function (error) {
        // either the download or the upload went wrong

        // resolve rather than reject the deferred object. Just log the error
        // so that it'll be analyzed later. In this way the process can
        // continue with the other resources
        deferred.resolve({
            success : false,
            oldResource : resource,
            newResource : null
        });
    });

    return deferred.promise;
}

/**
 * Downloads a resource
 * @param {String} postID the ID of the post which the resource belongs to
 * @param {String} resource the resource to download
 * @return {Q.Promise} a promise that will be resolved with the image data or
 * reject with the error occurred
 */
function downloadResource(postID, resource) {
    console.log('NET: downloading resource', resource);

    // this deferred object will take care of the download process
    var deferred = Q.defer();

    var resource = resource.indexOf('http') === 0 ? resource : 'http://www.macstories.net' +  resource;
    var remoteFilename = path.basename(resource);
    var localPath = '/tmp/' + postID + '_' + remoteFilename;

    var stream = fs.createWriteStream(localPath);

    // download the resource
    request(resource).pipe(stream).on('error', function (error) {
        deferred.reject(error);
    });

    // an error occurred
    stream.on('error', function (error) {
        deferred.reject(error);
    });

    // the file has been saved
    stream.on('close', function () {
        console.log('NET: downloaded resource', resource);

        deferred.resolve({
            resource : resource,
            path : localPath
        });
    });

    return deferred.promise;
}

/**
 * @param {String} resource the old resource which has been downloaded
 * @param {String} localPath filesystem's path of the resource to upload
 * @return {Q.Promise} a promise that will be resolved with new resource or
 * will be rejected with the error occurred
 */
function uploadResource(resource, localPath) {
    console.log('NET: uploading to Rackspace', resource);

    // this deferred object will take care of the upload process
    var deferred = Q.defer();

    // the base name of the file, used to name the remote file and to construct
    // the CDN URL
    var basename = path.basename(localPath);

    rackspaceClient.addFile(rackspaceBucketName, {
        remote: basename,
        local: localPath
    }, function (error, uploaded) {
        // remove the temporary file
        fs.unlink(localPath);

        if (error) {
            deferred.reject(error);
        } else {
            console.log('NET: uploaded to Rackspace resource', resource);
            deferred.resolve(rackspaceCDNURL + '/' + basename);
        }
    });

    return deferred.promise;
}

/**
 * Updates the content of a post, replacing the old resources with the new ones
 * @param {String} postId the ID of the post currently processed
 * @param {String} postContent the html content of the post
 * @param {Object} updates the updates to do on the post. It's a map of old
 * resources -> new resources
 * @return {String} the new content
 */
function replaceResources(postID, postContent, updates) {
    var keys = Object.keys(updates);

    // early exit
    if (keys.length === 0) {
        console.log('UPDATE: Post', postID, 'had no resources to update');
        return postContent;
    }

    var newContent = postContent;

    keys.forEach(function (oldResource, index, array) {
        // String.replace replaces only the first occurrence

        while (newContent.indexOf(oldResource) >= 0) {
            newContent = newContent.replace(oldResource, updates[oldResource]);
        }

    });

    console.log('UPDATE: Post', postID, 'updated', keys.length, 'resources');

    return newContent;
}

/**
 * Updates a given post in the database
 * @param {String} postID the ID of the post to update
 * @param {String} newContent the new content of the post
 */
function updatePost(postID, newContent) {
    var deferred = Q.defer();

    mysqlConnection.query(updatePostContentQuery, {
        postID : postID,
        postContent : newContent
    }, function (error, result) {
        if (error) {
            console.log('MYSQL: update failed due to error', error);

            deferred.reject(error);
        } else {
            console.log('MYSQL: correctly updated post', postID);

            deferred.resolve();
        }
    });

    return deferred.promise;
}

/**
 * Deletes resources from Rackspace
 * @param {Array[String]} resources an array of resources to delete
 */
function deleteResources(resources) {
    // just delete, without a great error policy: just log the error
    resources.forEach(function (resource, index, array) {
        rackspaceClient.destroyFile(rackspaceBucketName, resource, function (error, result) {
            if (error) {
                console.log('NET: couldn\'t delete resource', resource);
            }
        });
    });
}

function closeConnection(stats) {
    mysqlConnection.end(function () {
        console.log('MYSQL: connection closed');

        console.log('\n\nPROCESS COMPLETED\nIt took roughly',
                    ((new Date() - processStarted) / 1000) | 0, 'seconds\nStats:', stats);
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

