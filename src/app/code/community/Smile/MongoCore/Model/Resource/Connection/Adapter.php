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
 * This class allow developers to get manipulate collections from the MongoDB instance
 *
 * @category  Smile
 * @package   Smile_MongoCore
 * @author    Aurelien FOUCRET <aurelien.foucret@smile.fr>
 * @copyright 2013 Smile (http://www.smile-oss.com/)
 * @license   http://opensource.org/licenses/afl-3.0.php  Academic Free License (AFL 3.0)
 */
class Smile_MongoCore_Model_Resource_Connection_Adapter
{

    /**
     * When dealing with big dataset, MR operation need more than the default 30 seconds timeout.
     * This constant is used as default timout for such operations to give a MR job 1000 secs of running time
     *
     * @var int
     */
    const MR_COMMAND_TIMEOUT = 1000000;

    /**
     * The connection to the MongoDB instance
     *
     * @var Mongo
     */
    protected $_connection = null;


    /**
     * In this variable we load the configuration used to access to the db read
     * from the app/etc/local.xml into the document_db section
     *
     * @var array Config loaded from app/etc/local.xml
     */
    protected $_config = null;


    /**
     * Constructor which only init configuration
     *
     * @return void
     */
    public function __construct()
    {
        $this->_config = $this->_getConfig();
    }


    /**
     * Init a connection to the MongoDB instance.
     * Ensure DB settings are correctly set into app/etc/local.xml (connection_string and dbname are reqquired).
     *
     * If one mandatory configuration param is missing an exception is raised. The exception is not catched since this
     * kind of error can not be recovered nicely.
     *
     * @param int $retry The number of time the connection can be retried
     *
     * @throws MongoConnectionException If connection fails
     * @throws Mage_Core_Exception      If mandatory param are not set up
     *
     * @return Mongo The MongoDB database connection
     *
     */
    protected function _getConnection($retry=5)
    {

        if (!isset($this->_config['connection_string'])) {
            Mage::throwException('MongoDB is not configured yet (connection_string missing)');
        }

        if (!isset($this->_config['dbname'])) {
            Mage::throwException('MongoDB is not configured yet (dbname missing)');
        }

        if ($this->_connection === null) {
            try {
                $this->_connection = new MongoClient($this->_getConnectionString(), $this->_getConnectionOptions());
            } catch (MongoConnectionException $e) {
                if ($retry > 0) {
                    $this->_connection = null;
                    return $this->_getConnection($retry - 1);
                } else {
                    throw $e;
                }
            }
        }

        return $this->_connection;
    }

    /**
     * Return connection_string field of the configuration. Connection string sould be formatted as shown
     * into MongoDB PHP drive documentation (http://www.php.net/manual/fr/class.mongo.php) :
     *
     * mongodb://{$username}:{$password}@{$host}
     *
     * @return array The address of the MongoDB we want to connect on
     */
    protected function _getConnectionString()
    {
        return $this->_config['connection_string'] . $this->_config['dbname'];
    }


    /**
     * Return optionnal connection options given into the app/etc/local.xml file. Available connection option
     * can be found at http://www.php.net/manual/fr/mongoclient.construct.php
     *
     * Notes about important options :
     *
     *   - By default connection is initialized when new Mongo object is created. You can have a lazy
     *     connection by using 0 as value for the connect parameter (not recommended since we prefer the app
     *     crash if connection params are wrong).
     *
     * @return array An array that contains all options applied to the MongoDB connection
     */
    protected function _getConnectionOptions()
    {
        $options = array();

        if (isset($this->_config['connection_options']) && is_array($this->_config['connection_options'])) {
            $options = $this->_config['connection_options'];
        }

        return $options;
    }


    /**
     * Load the document_db configuration node from app/etc/local.xml and returns it as an array.
     *
     * The configuration should at least contain two mandatory settings documented above (connection_string and db_name)
     *
     * @return array The read configuration
     */
    protected function _getConfig()
    {
        return Mage::getConfig()->getNode('global/document_db')->asArray();
    }


    /**
     * Will returns the collection to be accessed into the current database
     *
     * Warning : MongoDB will create the collection if it does not exists yet. You have to be very careful about typo.
     *
     * TODO :(aufou) Let's see if it is possible to avoid collection to be created automaticaly.
     *
     * @param string $collectionName The name of the collection to be accessed
     *
     * @return MongoCollection
     *
     * @since 0.0.1
     */
    public function getCollection($collectionName)
    {
        return $this->_getConnection()->selectCollection($this->_config['dbname'], $collectionName);
    }


    /**
     * Run a map reduce job on a database collection and output it to a target collection.
     *
     * You can find more information about MR operations in MongoDB at the following address :
     *    - http://www.mongodb.org/display/DOCS/MapReduce
     *
     * @param string    $sourceCollection The name of the collection to be processed by the MR command.
     * @param string    $outputCollection The name of the collection where the output of the MR command will be put.
     * @param MongoCode $map              JS code of the map operation as a MongoCode object.
     * @param MongoCode $reduce           JS code of the reduce operation as a MongoCode object.
     *                                    If not specified use self::_getDefaultReducer() to retrieve identity reducer.
     * @param MongoCode $finalize         JS code of the finalize operation as a MongoCode object. The finalize function is optionnal.
     * @param array     $query            The query the source collection must be filtered with before mapping process.
     * @param string    $outputMode       The output mode of the operation (replace, merge or reduce). Default mode is replace.
     *                                    More information into MongoDB documentation.
     *
     * @return Smile_MongoCore_Model_Resource_Connection_Adapter Self reference
     *
     * @since 0.0.1
     */
    public function runMapReduce($sourceCollection, $outputCollection, $map, $reduce,
        $finalize = null, $query = null, $outputMode = 'replace'
    ) {
        $mrParams = array(
            'mapreduce' => $sourceCollection,
            'out'       => array($outputMode => $outputCollection),
            'map'       => $map,
            'reduce'    => $reduce
        );

        if (!is_null($finalize)) {
            $mrParams['finalize'] = $finalize;
        }

        if (!is_null($query)) {
            $mrParams['query'] = $query;
        }

        $db = $this->_getConnection()->selectDb($this->_config['dbname']);
        $lastCommandResult = $db->command($mrParams, array('timeout' => self::MR_COMMAND_TIMEOUT));

        if (!$lastCommandResult['ok']) {
            Mage::throwException(
                "Map reduce operation failed : {$lastCommandResult['assertion']} (code={$lastCommandResult['assertionCode']})"
            );
        }

        return $lastCommandResult;
    }

    /**
     * Util class to build MongoDB queries
     *
     * @return Smile_MongoCore_Model_Resource_Connection_Query_Builder
     */
    public function getQueryBuilder()
    {
        return Mage::getResourceSingleton('mongocore/connection_query_builder');
    }
}
