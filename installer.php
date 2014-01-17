<?php
/**
 * MongoGento
 *
 * NOTICE OF LICENSE
 *
 * This source file is subject to the Academic Free License (AFL 3.0)
 * that is bundled with this package in the file LICENSE_AFL.txt.
 * It is also available through the world-wide-web at this URL:
 * http://opensource.org/licenses/afl-3.0.php
 *
 * DISCLAIMER
 *
 * Do not edit or add to this file if you wish to upgrade MongoGento to newer
 * versions in the future.
 */

/**
 * MongoGento install script.
 *
 * @author    Aurelien FOUCRET <aurelien.foucret@smile.fr>
 * @copyright 2013 Smile (http://www.smile-oss.com/)
 * @license   http://opensource.org/licenses/afl-3.0.php  Academic Free License (AFL 3.0)
 */
// Git Repository

const GITHUB_REPO_OWNER = 'Smile-SA';
const GITHUB_REPO_NAME  = 'mongogento';
const GITHUB_API_BASE_URL = 'https://api.github.com';

/**
 * API Call to GitHub
 * 
 * @param string  $uri      URI of the REST API to be called
 * @param string  $method   Http method to use : GET / POST
 * @param mixed   $postData Data of the POST to use
 *
 * @return mixed
 */
function callGitHubApi($uri, $method = 'GET', $postData = null)  
{
    if (!isset($handle)) {
        $handle = curl_init();
    }

    $options = array(
        CURLOPT_HEADER         => false,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT        => 10,
        CURLOPT_URL            => GITHUB_API_BASE_URL . $uri,
        CURLOPT_CUSTOMREQUEST  => $method,
        CURLOPT_USERAGENT      => 'MongoGento Installer'
    );

    if ($postData !== null && $method == 'POST') {
        $options[CURLOPT_POSTFIELDS] = '$postData';
    }

    curl_setopt_array($handle, $options);
    $response = curl_exec($handle);

    if (!$response) {
        trigger_error(curl_error($handle));
    }

    $response = json_decode($response, true);
    
    return $response;
}

/**
 * Retrieve release list and release tarball URL from GitHub
 * 
 * @return array
 */
function getReleases() 
{
    $result = array('master' => 'https://github.com/' . GITHUB_REPO_OWNER . '/' . GITHUB_REPO_NAME . '/archive/master.tar.gz');

    $uri = '/repos/' . GITHUB_REPO_OWNER . '/' . GITHUB_REPO_NAME . '/releases';
    $releasesListResponse = callGitHubApi($uri);
    
    foreach ($releasesListResponse as $currentRelease) {
        $result[$currentRelease['tag_name']] = $currentRelease['tarball_url']; 
    }

    return $result;
}

/**
 * Download the archive file
 * 
 * @param string $downloadUri URL the file has to be downloaded from
 *
 * @return string
 */
function downloadRelease($downloadUri)
{  
    $outFile = dirname(__FILE__) . '/var/mongogento.tar.gz';

    echo "Start downloading mongogento archive at $downloadUri into $outFile ...\n";

    $fp = fopen($outFile, 'w');

    if ($fp === false) {
        trigger_error("Unable to write file $outFile");
        die;
    }

    $options = array(
        CURLOPT_USERAGENT      => 'MongoGento Installer',
        CURLOPT_TIMEOUT        => 50,
        CURLOPT_FILE           => $fp,
        CURLOPT_FOLLOWLOCATION => 1 
    );

    $handle = curl_init($downloadUri);
    curl_setopt_array($handle, $options);
    $data = curl_exec($handle);
 
    curl_close($handle);
    fclose($fp);

    echo "File downloaded.\n";
    return $outFile;
}


/**
  * Unpack the archive
  * 
  * @param string $archiveFile The path of the archive to be extracted
  *
  * @return void
  */
function extractArchive($archiveFile) 
{
    echo "Unpacking archive ...\n";
    $cmd = "tar -zxf $archiveFile --strip 2 --wildcards */src";
    $test = shell_exec($cmd);
    echo "Unpacked finished.";
}


$releases = getReleases();

// Use master release by default
$selectedRelease = 'master';

// Check if a release as been selected manually
if (isset($argv[1])) {
    $selectedRelease = $argv[1];
}

// Fails if selected release does not exists
if (!isset($releases[$selectedRelease])) {
    echo "Error : Release $selectedRelease does not exist\n";
    die;
}

$archiveFile = downloadRelease($releases[$selectedRelease]);
extractArchive($archiveFile);

echo "MongoGento sucessfully installed.\n";
